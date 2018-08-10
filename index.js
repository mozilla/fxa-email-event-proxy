// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, you can obtain one at https://mozilla.org/MPL/2.0/.

'use strict'

const crypto = require('crypto')
const Promise = require('bluebird')
const sqs = require('sqs')

const { AUTH, SQS_SUFFIX, PROVIDER } = process.env

if (! AUTH || ! SQS_SUFFIX || ! PROVIDER) {
  throw new Error('Missing config')
}

if (PROVIDER !== 'sendgrid' && PROVIDER !== 'socketlabs') { 
  throw new Error('Only the following providers are supported: sendgrid, socketlabs')
}

const AUTH_HASH = createHash(AUTH).split('')

const QUEUES = {
  Bounce: `fxa-email-bounce-${SQS_SUFFIX}`,
  Complaint: `fxa-email-complaint-${SQS_SUFFIX}`,
  Delivery: `fxa-email-delivery-${SQS_SUFFIX}`
}

// env vars: SQS_ACCESS_KEY, SQS_SECRET_KEY, SQS_REGION
const SQS_CLIENT = sqs()
SQS_CLIENT.pushAsync = Promise.promisify(SQS_CLIENT.push)

module.exports = { main }

async function main (data) {
  try {
    // If there's a body, it's a request from the API gateway
    if (data.body) {
      // Requests from the API gateway must be authenticated
      if (! data.queryStringParameters || ! authenticate(data.queryStringParameters.auth)) {
        return {
          statusCode: 401,
          body: 'Unauthorized',
          isBase64Encoded: false
        }
      }

      data = JSON.parse(data.body)
    }

    if (! Array.isArray(data)) {
      data = [ data ]
    }

    let results = await processEvents(data)
    return {
      statusCode: 200,
      body: `Processed ${results.length} events`,
      isBase64Encoded: false
    }
  } catch(error) {
    return {
      statusCode: 500,
      body: 'Internal Server Error',
      isBase64Encoded: false
    }
  }
}

function authenticate (auth) {
  const authHash = createHash(auth)
  return AUTH_HASH
    .reduce((equal, char, index) => equal && char === authHash[index], true)
}

function createHash (value) {
  const hash = crypto.createHash('sha256')
  hash.update(value)
  return hash.digest('base64')
}

async function processEvents (events) {
  return Promise.all(
    events.map(marshallEvent)
      .filter(event => !! event)
      .map(sendEvent)
  )
}

const marshallEvent = require(`./${PROVIDER}`)

function sendEvent (event) {
  return SQS_CLIENT.pushAsync(QUEUES[event.notificationType], event)
    .then(() => console.log('Sent:', event.notificationType))
    .catch(error => {
      console.error('Failed to send event:', event)
      console.error(error.stack)
      throw error
    })
}
