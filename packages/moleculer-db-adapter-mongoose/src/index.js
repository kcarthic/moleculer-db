/*
 * moleculer-db-adapter-mongoose
 * Copyright (c) 2019 MoleculerJS (https://github.com/moleculerjs/moleculer-db)
 * MIT Licensed
 */

"use strict";

const _ 		= require("lodash");
const Promise	= require("bluebird");
const { ServiceSchemaError } = require("moleculer").Errors;
const mongoose  = require("mongoose");

////mongoose-diff-history code
const omit = require('omit-deep');
const pick = require('lodash.pick');
const empty = require('deep-empty-object');
const { assign } = require('power-assign');
// try to find an id property, otherwise just use the index in the array
const objectHash = (obj, idx) => obj._id || obj.id || `$$index: ${idx}`;
const diffPatcher = require('jsondiffpatch').create({ objectHash });

mongoose.set("useNewUrlParser", true);
mongoose.set("useFindAndModify", false);
mongoose.set("useCreateIndex", true);

class MongooseDbAdapter {

	/**
	 * Creates an instance of MongooseDbAdapter.
	 * @param {String} uri
	 * @param {Object?} opts
	 *
	 * @memberof MongooseDbAdapter
	 */
	constructor(uri, opts) {
		if(opts.HistorySchema){
			this.historySchema = new mongoose.Schema(
										{
											collectionName: String,
											collectionId: Schema.Types.ObjectId,
											diff: {},
											user: {},
											reason: String,
											version: { type: Number, min: 0 }
										},
										{
											timestamps: true
										}
									);
			this.historyModel = mongoose.model(opts.HistorySchema, this.historySchema);
			delete opts.HistorySchema;
		}
		this.uri = uri,
		this.opts = opts;
		mongoose.Promise = Promise;
	}

	/**
	 * Initialize adapter
	 *
	 * @param {ServiceBroker} broker
	 * @param {Service} service
	 *
	 * @memberof MongooseDbAdapter
	 */
	init(broker, service) {
		this.broker = broker;
		this.service = service;

		if (this.service.schema.model) {
			this.model = this.service.schema.model;
		} else if (this.service.schema.schema) {
			if (!this.service.schema.modelName) {
				throw new ServiceSchemaError("`modelName` is required when `schema` is given in schema of service!");
			}
			this.schema = this.service.schema.schema;
			this.modelName = this.service.schema.modelName;
		}

		if (!this.model && !this.schema) {
			/* istanbul ignore next */
			throw new ServiceSchemaError("Missing `model` or `schema` definition in schema of service!");
		}
		
		//Added from mongoose-diff-history
		this.schema.pre('save', function (next) {
			if (this.isNew) return next();
			this.constructor
				.findOne({ _id: this._id })
				.then(original => {
					if (checkRequired(opts, {}, this)) {
						return;
					}
					return saveDiffObject(this, original, this.toObject({ depopulate: true }), opts);
				})
				.then(() => next())
				.catch(next);
		});

		this.schema.pre('findOneAndUpdate', function (next) {
			if (checkRequired(opts, this)) {
				return next();
			}
			saveDiffs(this, opts)
				.then(() => next())
				.catch(next);
		});

		this.schema.pre('update', function (next) {
			if (checkRequired(opts, this)) {
				return next();
			}
			saveDiffs(this, opts)
				.then(() => next())
				.catch(next);
		});

		this.schema.pre('updateOne', function (next) {
			if (checkRequired(opts, this)) {
				return next();
			}
			saveDiffs(this, opts)
				.then(() => next())
				.catch(next);
		});

		this.schema.pre('remove', function (next) {
			if (checkRequired(opts, this)) {
				return next();
			}
			saveDiffObject(this, this, {}, opts)
				.then(() => next())
				.catch(next);
		});
		
	}

	/**
	 * Connect to database
	 *
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	connect() {
		let conn;

		if (this.model) {
			/* istanbul ignore next */
			if (mongoose.connection.readyState == 1) {
				this.db = mongoose.connection;
				return Promise.resolve();
			} else if (mongoose.connection.readyState == 2) {
				conn = mongoose.connection;
			} else {
				conn = mongoose.connect(this.uri, this.opts);
			}
		} else if (this.schema) {
			conn = mongoose.createConnection(this.uri, this.opts);
			this.model = conn.model(this.modelName, this.schema);
		}

		return conn.then(result => {
			this.conn = conn;

			if (result.connection)
				this.db = result.connection.db;
			else
				this.db = result.db;

			this.service.logger.info("MongoDB adapter has connected successfully.");

			/* istanbul ignore next */
			this.db.on("disconnected", () => this.service.logger.warn("Mongoose adapter has disconnected."));
			this.db.on("error", err => this.service.logger.error("MongoDB error.", err));
			this.db.on("reconnect", () => this.service.logger.info("Mongoose adapter has reconnected."));
		});
	}

	/**
	 * Disconnect from database
	 *
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	disconnect() {
		return new Promise(resolve => {
			if (this.db && this.db.close) {
				this.db.close(resolve);
			} else {
				mongoose.connection.close(resolve);
			}
		});
	}

	/**
	 * Find all entities by filters.
	 *
	 * Available filter props:
	 * 	- limit
	 *  - offset
	 *  - sort
	 *  - search
	 *  - searchFields
	 *  - query
	 *
	 * @param {any} filters
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	find(filters) {
		return this.createCursor(filters).exec();
	}

	/**
	 * Find an entity by query
	 *
	 * @param {Object} query
	 * @returns {Promise}
	 * @memberof MemoryDbAdapter
	 */
	findOne(query) {
		return this.model.findOne(query).exec();
	}

	/**
	 * Find an entities by ID
	 *
	 * @param {any} _id
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	findById(_id) {
		return this.model.findById(_id).exec();
	}

	/**
	 * Find any entities by IDs
	 *
	 * @param {Array} idList
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	findByIds(idList) {
		return this.model.find({
			_id: {
				$in: idList
			}
		}).exec();
	}

	/**
	 * Get count of filtered entites
	 *
	 * Available filter props:
	 *  - search
	 *  - searchFields
	 *  - query
	 *
	 * @param {Object} [filters={}]
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	count(filters = {}) {
		return this.createCursor(filters).countDocuments().exec();
	}

	/**
	 * Insert an entity
	 *
	 * @param {Object} entity
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	insert(entity) {
		const item = new this.model(entity);
		return item.save();
	}

	/**
	 * Insert many entities
	 *
	 * @param {Array} entities
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	insertMany(entities) {
		return this.model.create(entities);
	}

	/**
	 * Update many entities by `query` and `update`
	 *
	 * @param {Object} query
	 * @param {Object} update
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	updateMany(query, update) {
		return this.model.updateMany(query, update, { multi: true, "new": true }).then(res => res.n);
	}

	/**
	 * Update an entity by ID and `update`
	 *
	 * @param {any} _id
	 * @param {Object} update
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	updateById(_id, update) {
		return this.model.findByIdAndUpdate(_id, update, { "new": true });
	}

	/**
	 * Remove entities which are matched by `query`
	 *
	 * @param {Object} query
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	removeMany(query) {
		return this.model.deleteMany(query).then(res => res.n);
	}

	/**
	 * Remove an entity by ID
	 *
	 * @param {any} _id
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	removeById(_id) {
		return this.model.findByIdAndRemove(_id);
	}

	/**
	 * Clear all entities from collection
	 *
	 * @returns {Promise}
	 *
	 * @memberof MongooseDbAdapter
	 */
	clear() {
		return this.model.deleteMany({}).then(res => res.n);
	}

	/**
	 * Convert DB entity to JSON object
	 *
	 * @param {any} entity
	 * @returns {Object}
	 * @memberof MongooseDbAdapter
	 */
	entityToObject(entity) {
		let json = entity.toJSON();
		if (entity._id && entity._id.toHexString) {
			json._id = entity._id.toHexString();
		} else if (entity._id && entity._id.toString) {
			json._id = entity._id.toString();
		}
		return json;
	}

	/**
	 * Create a filtered query
	 * Available filters in `params`:
	 *  - search
	 * 	- sort
	 * 	- limit
	 * 	- offset
	 *  - query
	 *
 	 * @param {Object} params
	 * @returns {MongoQuery}
	 */
	createCursor(params) {
		if (params) {
			const q = this.model.find(params.query);

			// Search
			if (_.isString(params.search) && params.search !== "") {
				if (params.searchFields && params.searchFields.length > 0) {
					q.find({
						$or: params.searchFields.map(f => (
							{
								[f]: new RegExp(params.search, "i")
							}
						))
					});
				} else {
					// Full-text search
					// More info: https://docs.mongodb.com/manual/reference/operator/query/text/
					q.find({
						$text: {
							$search: params.search
						}
					});
					q._fields = {
						_score: {
							$meta: "textScore"
						}
					};
					q.sort({
						_score: {
							$meta: "textScore"
						}
					});
				}
			}

			// Sort
			if (_.isString(params.sort))
				q.sort(params.sort.replace(/,/, " "));
			else if (Array.isArray(params.sort))
				q.sort(params.sort.join(" "));

			// Offset
			if (_.isNumber(params.offset) && params.offset > 0)
				q.skip(params.offset);

			// Limit
			if (_.isNumber(params.limit) && params.limit > 0)
				q.limit(params.limit);

			return q;
		}
		return this.model.find();
	}

	/**
	* Transforms 'idField' into MongoDB's '_id'
	* @param {Object} entity
	* @param {String} idField
	* @memberof MongoDbAdapter
	* @returns {Object} Modified entity
	*/
	beforeSaveTransformID (entity, idField) {
		let newEntity = _.cloneDeep(entity);

		if (idField !== "_id" && entity[idField] !== undefined) {
			newEntity._id = this.stringToObjectID(newEntity[idField]);
			delete newEntity[idField];
		}

		return newEntity;
	}

	/**
	* Transforms MongoDB's '_id' into user defined 'idField'
	* @param {Object} entity
	* @param {String} idField
	* @memberof MongoDbAdapter
	* @returns {Object} Modified entity
	*/
	afterRetrieveTransformID (entity, idField) {
		if (idField !== "_id") {
			entity[idField] = this.objectIDToString(entity["_id"]);
			delete entity._id;
		}
		return entity;
	}

	/**
	* Convert hex string to ObjectID
	* @param {String} id
	* @returns ObjectID}
	* @memberof MongooseDbAdapter
	*/
	stringToObjectID (id) {
		if (typeof id == "string" && mongoose.Types.ObjectId.isValid(id))
			return new mongoose.Schema.Types.ObjectId(id);
		return id;
	}

	/**
	* Convert ObjectID to hex string
	* @param {ObjectID} id
	* @returns {String}
	* @memberof MongooseDbAdapter
	*/
	objectIDToString (id) {
		if(id && id.toString)
			return id.toString();
		return id;
	}

	/**
	* check if required parameters available for diff history
	* @param {Object} opts
	* @param {Object} queryObject
	* @param {Object} updatedObject
	* @returns {Boolean}
	* @memberof MongooseDbAdapter
	*/
	//https://eslint.org/docs/rules/complexity#when-not-to-use-it
	/* eslint-disable complexity */
	checkRequired(opts, queryObject, updatedObject) {
		if(!this.historyModel) 
			return; //Do Not check if history model is not defined
		if (queryObject && !queryObject.options && !updatedObject) {
			return;
		}
		const { __user: user, __reason: reason } =
			(queryObject && queryObject.options) || updatedObject;
		if (
			opts.required &&
			((opts.required.includes('user') && !user) ||
				(opts.required.includes('reason') && !reason))
		) {
			return true;
		}
	}
	
	saveDiffObject(currentObject, original, updated, opts, queryObject) {
		if(!this.historyModel) 
			return; //Do Not save the diff if history model is not defined
		const { __user: user, __reason: reason, __session: session } =
			(queryObject && queryObject.options) || currentObject;

		let diff = diffPatcher.diff(
			JSON.parse(JSON.stringify(original)),
			JSON.parse(JSON.stringify(updated))
		);

		if (opts.omit) {
			omit(diff, opts.omit, { cleanEmpty: true });
		}

		if (opts.pick) {
			diff = pick(diff, opts.pick);
		}

		if (!diff || !Object.keys(diff).length || empty.all(diff)) {
			return;
		}

		const collectionId = currentObject._id;
		const collectionName = this.model.collection.collectionName;

		return History.findOne({ collectionId, collectionName })
			.sort('-version')
			.then(lastHistory => {
				const history = new this.historyModel({
					collectionId,
					collectionName,
					diff,
					user,
					reason,
					version: lastHistory ? lastHistory.version + 1 : 0
				});
				if (session) {
					return history.save({ session });
				}
				return history.save();
			});
	}
	
	/* eslint-disable complexity */
	const saveDiffHistory = (queryObject, currentObject, opts) => {
		if(!this.historyModel) 
			return; //Do Not save the diff if history model is not defined
		
		const queryUpdate = queryObject.getUpdate();
		const schemaOptions = queryObject.model.schema.options || {};

		let keysToBeModified = [];
		let mongoUpdateOperations = [];
		let plainKeys = [];

		for (const key in queryUpdate) {
			const value = queryUpdate[key];
			if (key.startsWith('$') && typeof value === 'object') {
				const innerKeys = Object.keys(value);
				keysToBeModified = keysToBeModified.concat(innerKeys);
				if (key !== '$setOnInsert') {
					mongoUpdateOperations = mongoUpdateOperations.concat(key);
				}
			} else {
				keysToBeModified = keysToBeModified.concat(key);
				plainKeys = plainKeys.concat(key);
			}
		}

		const dbObject = pick(currentObject, keysToBeModified);
		let updatedObject = assign(
			dbObject,
			pick(queryUpdate, mongoUpdateOperations),
			pick(queryUpdate, plainKeys)
		);

		let { strict } = queryObject.options || {};
		// strict in Query options can override schema option
		strict = strict !== undefined ? strict : schemaOptions.strict;

		if (strict === true) {
			const validPaths = Object.keys(queryObject.model.schema.paths);
			updatedObject = pick(updatedObject, validPaths);
		}

		return saveDiffObject(
			currentObject,
			dbObject,
			updatedObject,
			opts,
			queryObject
		);
	};
	
	const saveDiffs = (queryObject, opts) =>
		queryObject
			.find(queryObject._conditions)
			.cursor()
			.eachAsync(result => saveDiffHistory(queryObject, result, opts));
	
	const getVersion = (id, version, queryOpts, cb) => {
		if(!this.historyModel) 
			return; //Do Not save the diff if history model is not defined
		
		if (typeof queryOpts === 'function') {
			cb = queryOpts;
			queryOpts = undefined;
		}

		return this.model
			.findById(id, null, queryOpts)
			.then(latest => {
				latest = latest || {};
				return this.historyModel.find(
					{
						collectionName: this.model.collection.collectionName,
						collectionId: id,
						version: { $gte: parseInt(version, 10) }
					},
					{ diff: 1, version: 1 },
					{ sort: '-version' }
				)
					.lean().cursor()
					.eachAsync(history => {
						diffPatcher.unpatch(latest, history.diff);
					})
					.then(() => {
						if (isValidCb(cb)) return cb(null, latest);
						return latest;
					});
			})
			.catch(err => {
				if (isValidCb(cb)) return cb(err, null);
				throw err;
			});
	};
	
	const getDiffs = (id, opts, cb) => {
		if(!this.historyModel) 
			return; //Do Not save the diff if history model is not defined
		
		opts = opts || {};
		if (typeof opts === 'function') {
			cb = opts;
			opts = {};
		}
		return this.historyModel.find(
			{ collectionName: this.model.collection.collectionName, collectionId: id },
			null, opts)
			.lean()
			.then(histories => {
				if (isValidCb(cb)) return cb(null, histories);
				return histories;
			})
			.catch(err => {
				if (isValidCb(cb)) return cb(err, null);
				throw err;
			});
	};
	
	const getHistories = (id, expandableFields, cb) => {
		if(!this.historyModel) 
			return; //Do Not save the diff if history model is not defined
		
		expandableFields = expandableFields || [];
		if (typeof expandableFields === 'function') {
			cb = expandableFields;
			expandableFields = [];
		}

		const histories = [];

		return this.historyModel.find({ collectionName: this.model.collection.collectionName, collectionId: id })
			.lean().cursor()
			.eachAsync(history => {
				const changedValues = [];
				const changedFields = [];
				for (const key in history.diff) {
					if (history.diff.hasOwnProperty(key)) {
						if (expandableFields.indexOf(key) > -1) {
							const oldValue = history.diff[key][0];
							const newValue = history.diff[key][1];
							changedValues.push(
								key + ' from ' + oldValue + ' to ' + newValue
							);
						} else {
							changedFields.push(key);
						}
					}
				}
				const comment =
					'modified ' + changedFields.concat(changedValues).join(', ');
				histories.push({
					changedBy: history.user,
					changedAt: history.createdAt,
					updatedAt: history.updatedAt,
					reason: history.reason,
					comment: comment
				});
			})
			.then(() => {
				if (isValidCb(cb)) return cb(null, histories);
				return histories;
			})
			.catch(err => {
				if (isValidCb(cb)) return cb(err, null);
				throw err;
			});
	};
}

module.exports = MongooseDbAdapter;
