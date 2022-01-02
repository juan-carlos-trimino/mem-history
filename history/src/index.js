/***
It records the user's viewing history.
video-streaming -> RabbitMQ('viewed' message) -> history -> mongoDB('history' db)

* The app contains a single RabbitMQ server instance; the RabbitMQ server contains multiple queues
  with different names.
* Each microservice has its own private database; the databases are hosted on a shared server.

(1) 'viewed' message is how the video-streaming microservice informs the history microservice
     that the user has watched a video.
(2) The history microservice receives messages from the video-streaming microservice, and it
    records them in its own database.
***/
const express = require("express");
const mongodb = require("mongodb");
const mongodbClient = require('mongodb').MongoClient;
const amqp = require("amqplib");
const bodyParser = require("body-parser");
const winston = require('winston');

/******
Globals
******/
//Create a new express instance.
const app = express();
const SVC_NAME = "history";
const DB_NAME = process.env.DB_NAME;
const SVC_DNS_DB = process.env.SVC_DNS_DB;
const SVC_DNS_RABBITMQ = process.env.SVC_DNS_RABBITMQ;
const PORT = process.env.PORT && parseInt(process.env.PORT) || 3000;
const MAX_RETRIES = process.env.MAX_RETRIES && parseInt(process.env.MAX_RETRIES) || 10;
let READINESS_PROBE = false;

/***
Resume Operation
----------------
The resume operation strategy intercepts unexpected errors and responds by allowing the process to
continue.
***/
process.on('uncaughtException',
err => {
  logger.error(`${SVC_NAME} - Uncaught exception.`);
  logger.error(`${SVC_NAME} - ${err}`);
  logger.error(`${SVC_NAME} - ${err.stack}`);
})

/***
Abort and Restart
-----------------
***/
// process.on("uncaughtException",
// err => {
//   console.error("Uncaught exception:");
//   console.error(err && err.stack || err);
//   process.exit(1);
// })

//Winston requires at least one transport (location to save the log) to create a log.
const logConfiguration = {
  transports: [ new winston.transports.Console() ],
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSSSS' }),
    winston.format.printf(msg => `${msg.timestamp} ${msg.level} ${msg.message}`)
  ),
  exitOnError: false
}

//Create a logger and pass it the Winston configuration object.
const logger = winston.createLogger(logConfiguration);

/***
Unlike most other programming languages or runtime environments, Node.js doesn't have a built-in
special "main" function to designate the entry point of a program.

Accessing the main module
-------------------------
When a file is run directly from Node.js, require.main is set to its module. That means that it is
possible to determine whether a file has been run directly by testing require.main === module.
***/
if (require.main === module) {
  main()
  .then(() => {
    READINESS_PROBE = true;
    logger.info(`${SVC_NAME} - Microservice is listening on port "${PORT}"!`);
  })
  .catch(err => {
    logger.error(`${SVC_NAME} - Microservice failed to start.`);
    logger.error(`${SVC_NAME} - ${err}`);
    logger.error(`${SVC_NAME} - ${err.stack}`);
  });
}

function main() {
  //Throw exception if any required environment variables are missing.
  if (process.env.SVC_DNS_DB === undefined) {
    throw new Error('Please specify the service DNS for the database in the environment variable SVC_DNS_DB.');
  }
  else if (process.env.DB_NAME === undefined) {
    throw new Error('Please specify the name of the database in the environment variable DB_NAME.');
  }
  else if (process.env.SVC_DNS_RABBITMQ === undefined) {
    throw new Error('Please specify the name of the service DNS for RabbitMQ in the environment variable SVC_DNS_RABBITMQ.');
  }
  //Display a message if any optional environment variables are missing.
  else {
    if (process.env.PORT === undefined) {
      logger.info(`${SVC_NAME} - The environment variable PORT for the HTTP server is missing; using port ${PORT}.`);
    }
    //
    if (process.env.MAX_RETRIES === undefined) {
      logger.info(`${SVC_NAME} - The environment variable MAX_RETRIES is missing; using MAX_RETRIES=${MAX_RETRIES}.`);
    }
  }
  return requestWithRetry(connectToDb, SVC_DNS_DB, MAX_RETRIES)  //Connect to the database...
  .then(dbConn => {                                              //then...
    return requestWithRetry(connectToRabbitMQ, SVC_DNS_RABBITMQ, MAX_RETRIES)  //connect to RabbitMQ...
    .then(conn => {
      //Create a RabbitMQ messaging channel.
      return conn.createChannel();
    })
    .then(channel => {                          //then...
      return startHttpServer(dbConn, channel);  //start the HTTP server.
    });
  });
}

function connectToDb(url, currentRetry) {
  logger.info(`${SVC_NAME} - Connecting (${currentRetry}) to 'MongoDB' at ${url}/database(${DB_NAME}).`);
  return mongodbClient
  .connect(url, { useUnifiedTopology: true })
  .then(client => {
    logger.info(`${SVC_NAME} - Connected to mongodb database '${DB_NAME}'.`);
    return client.db(DB_NAME);
  });
}

function connectToRabbitMQ(url, currentRetry) {
  logger.info(`${SVC_NAME} - Connecting (${currentRetry}) to 'RabbitMQ' at ${url}.`);
  return amqp.connect(url)
  .then(conn => {
    logger.info(`${SVC_NAME} - Connected to RabbitMQ.`);
    return conn;
  });
}

async function sleep(timeout) {
  return new Promise(resolve => {
    setTimeout(() => { resolve(); }, timeout);
  });
}

async function requestWithRetry(func, url, maxRetry) {
  for (let currentRetry = 0;;) {
    try {
      ++currentRetry;
      return await func(url, currentRetry);
    }
    catch(err) {
      if (currentRetry === maxRetry) {
        //Save the error from the most recent attempt.
        lastError = err;
        logger.info(`${SVC_NAME} - Maximum number of ${maxRetry} retries has been reached.`);
        break;
      }
      const timeout = (Math.pow(2, currentRetry) - 1) * 100;
      logger.info(`${SVC_NAME} - Waiting ${timeout}ms...`);
      await sleep(timeout);
    }
  }
  //Throw the error from the last attempt; let the error bubble up to the caller.
  throw lastError;
}

//Start the HTTP server.
function startHttpServer(db, channel) {
  //Notify when the server has started.
  return new Promise(resolve => {
    app.use(bodyParser.json());  //Enable JSON body for HTTP requests.
    setupHandlers(db, channel);
    app.listen(PORT,
    () => {
      resolve();  //HTTP server is listening, resolve the promise.
    });
  });
}

//Setup event handlers.
function setupHandlers(db, channel) {
  //Readiness probe.
  app.get('/readiness',
  (req, res) => {
    res.sendStatus(READINESS_PROBE === true ? 200 : 500);
  });
  //
  const videosCollection = db.collection('videos');
  //HTTP GET API to retrieve video viewing history.
  app.get('/videos',
  (req, res) => {
    const cid = req.headers['X-Correlation-Id'];
    videosCollection.find()  //Retrieve video list from database.
    .toArray()             //In a real application this should be paginated.
    .then(videos => {
      logger.info(`${SVC_NAME} ${cid} - Retrieved the viewing history from the database.`);
      res.json({ videos });
    })
    .catch(err => {
      logger.error(`${SVC_NAME} ${cid} - Failed to retrieve the viewing history from the database.`);
      logger.error(`${SVC_NAME} ${cid} - ${err}`);
      res.sendStatus(500);
    });
  });
  //
  //Function to handle incoming messages.
  function consumeViewedMessage(msg) {
    /***
    Parse the JSON message to a JavaScript object.
    RabbitMQ doesn't natively support JSON. RabbitMQ is actually agnostic about the format for the
    message payload, and from its point of view, a message is just a blob of binary data.
    ***/
    const parsedMsg = JSON.parse(msg.content.toString());
    const cid = parsedMsg.video.cid;
    logger.info(`${SVC_NAME} ${cid} - Received a "viewed" message.`);
    return videosCollection
    //Record the view in the history database.
    .insertOne({ videoId: parsedMsg.video.id, watched: new Date() })
    .then(() => {
      logger.info(`${SVC_NAME} ${cid} - Acknowledging message was handled.`);
      channel.ack(msg);  //If there is no error, acknowledge the message.
    });
  };
  /***
  'Assert' the 'viewed' exchange rather than the 'viewed' queue.
  (1) Once connected, 'assert' (not 'create') the message queue and start pulling messages from the
      queue. The different between 'assert' and 'create' the message queue is that multiple
      microservices can 'assert' a queue; i.e., check for the existence of the queue and then only
      creating it when it doesn't already exist. That means the queue is created once and shared
      between all participating microservices.
  (2) Single-recipient messages are one-to-one; a message is sent from one microservice and
      received by only a single microservice. In this configuration, there might be multiple
      senders and receivers, but only one receiver is guaranteed to consume each individual
      message.
  (3) Multi-recipient messages are one-to-many (broadcast or notifications); a message is sent from
      only a single microservice but potentially received by many others. To use multi-recipient
      messages with RabbitMQ, a message exchange is required.
  ***/
  /***
  return channel.assertExchange('viewed', 'fanout')
  .then(() =>
  {
    //(1) Create an anonymous queue. The option 'exclusive' is set to 'true' so that the queue
    //    will be deallocated automatically when the microservice disconnects from it; otherwise,
    //    the app will have a memory leak.
    //(2) By creating an unnamed queue, a queue is created uniquely for a microservice. The
    //    'viewed' exchange is shared among all microservices, but the anonymous queue is owned
    //    solely by a microservice.
    //(3) In creating the unnamed queue, a random name generated by RabbitMQ is returned. The
    //    name that RabbitMQ assigned to the queue is only important because the queue must be
    //    binded to the 'viewed' exchange. This binding connects the exchange and the queue, such
    //    that RabbitMQ messages published on the exchange are routed to the queue.
    //(4) Every other microservice that wants to receive the 'viewed' message creates its own
    //    unnamed queue to bind to the 'viewed' exchange. There can be any number of other
    //    microservices bound to the 'viewed' exchange, and they will all receive copies of
    //    messages on their own anonymous queues as messages are published to the exchange.
    return channel.assertQueue('', { exclusive: true });
  })
  .then(response =>
  {
    //Assign the anonymous queue an automatically generated unique identifier for its name.
    const queueName = response.queue;
    console.log(`Created anonymous queue ${queueName}, binding it to 'viewed' exchange.`);
    //Bind the queue to the exchange.
    return channel.bindQueue(queueName, 'viewed', '')
      .then(() =>
      {
        //Start receiving messages from the anonymous queue that's bound to the 'viewed'
        //exchange.
        return channel.consume(queueName, consumeViewedMessage);
      });
  });
  ***/
  return channel.assertQueue('viewed', { exclusive: false })
  .then(() => {
    return channel.consume('viewed', consumeViewedMessage);
  });
}
