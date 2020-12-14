// Databricks notebook source
// /FileStore/tables/ratings.csv

//creating an rdd from external datasets
val data = sc.textFile("/FileStore/tables/ratings.csv")

// COMMAND ----------

data.collect()

// COMMAND ----------

val ratingsData = data.map( x => x.split(",")(2))

ratingsData.collect()

// COMMAND ----------

ratingsData.count()

// COMMAND ----------

ratingsData.countByValue()

// COMMAND ----------


