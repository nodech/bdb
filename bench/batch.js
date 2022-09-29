'use strict';

const Path = require('path');
const crypto = require('crypto');
const util = require('./util');
const bdb = require('../lib/bdb');

const argv = process.argv.slice();

const BATCHES = Number(argv[3]) || 300;
const PUTS = Number(argv[4]) || 300;
const DELS = Number(argv[5]) || 200;

const getPrefix = name => Path.resolve(__dirname, `bdb-${name}`);

const MAX_KEY_SIZE = 100;
const MAX_VALUE_SIZE = 20000;

const randomBytesLen = (max) => {
  const len = crypto.randomInt(max);
  return crypto.randomBytes(len);
};

const randomKey = () => randomBytesLen(MAX_KEY_SIZE);
const randomValue = () => randomBytesLen(MAX_VALUE_SIZE);

async function batchBench(batch) {
  if (DELS > PUTS)
    throw new Error('DELS must be lower than PUTS');

  const puts = [];
  const dels = [];

  for (let i = 0; i < BATCHES; i++) {
    for (let j = 0; j < PUTS; j++) {
      const key = randomKey();
      const value = randomValue();
      puts.push([key, value]);

      if (j < DELS)
        dels.push(key);
    }
  }

  console.log('Running puts...');
  util.logMemory();
  const beforePuts = util.now();

  for (let i = 0; i < BATCHES; i++) {
    const b = batch();
    for (let j = 0; j < PUTS; j++) {
      const entry = puts[(i * PUTS) + j];
      b.put(entry[0], entry[1]);
    }
    await b.write();
  }

  const afterPuts = util.now();
  util.logMemory();
  console.log('PUTS: ', afterPuts - beforePuts);
  console.log('Running dels...');

  const beforeDels = util.now();
  for (let i = 0; i < BATCHES; i++) {
    const b = batch();
    for (let j = 0; j < DELS; j++) {
      const key = dels[(i * DELS) + j];
      b.del(key);
    }
    await b.write();
  }

  const afterDels = util.now();
  console.log('DELS: ', afterDels - beforeDels);
  util.logMemory();
}

async function runBench(memory, chain) {
  const location = getPrefix(chain ? 'chainedBatch' : 'batch');
  const db = bdb.create({ memory, location });

  await db.open();
  if (chain)
    await batchBench(db.chainedBatch.bind(db));
  else
    await batchBench(db.batch.bind(db));
  await db.close();
}

(async () => {
  const argv = process.argv.slice();
  const arg = argv[2];
  const cases = [
    'batch',
    'chained-batch',
    'mem-batch',
    'mem-chained-batch'
  ];

  switch (arg) {
    case 'batch':
      console.log('Benchmarking batch (disk)...');
      await runBench(false, false);
      break;

    case 'chained-batch':
      console.log('Benchmarking chained batch (disk)...');
      await runBench(false, true);
      break;

    case 'mem-batch':
      console.log('Benchmarking batch (memory)...');
      await runBench(true, false);
      break;

    case 'mem-chained-batch':
      console.log('Benchmarking chained batch (memory)...');
      await runBench(true, true);
      break;

    default:
      console.log('Choose from: ', cases.join(', '));
  }
})().catch((err) => {
  console.error(err.stack);
  process.exit(1);
});
