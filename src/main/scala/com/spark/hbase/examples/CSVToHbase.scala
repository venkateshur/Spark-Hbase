package com.spark.hbase.examples

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase._
import org.slf4j.LoggerFactory

case class ContactRecord(rowkey: String, officeAddress: String, officePhone: String, personalName: String, personalPhone: String)

object CSVToHbase extends App{

  val logger = LoggerFactory.getLogger(this.getClass.getName)
  val spark = initSparkSession
  logger.info(s"Reading input from the path: $args(0)")
  val inputDf = readCSV(spark,args(0))
  logger.info(s"writing data to hbase")
  writeToHbase(inputDf, catalog)

  private def initSparkSession = SparkSession.builder().appName("CSV-HBASE").getOrCreate()


  private def catalog = s"""{
                   |"table":{"namespace":"default", "name":"Contacts"},
                   |"rowkey":"key",
                   |"columns":{
                   |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
                   |"officeAddress":{"cf":"Office", "col":"Address", "type":"string"},
                   |"officePhone":{"cf":"Office", "col":"Phone", "type":"string"},
                   |"personalName":{"cf":"Personal", "col":"Name", "type":"string"},
                   |"personalPhone":{"cf":"Personal", "col":"Phone", "type":"string"}
                   |}
                   |}""".stripMargin

  //Reading from Hbase
  private def withCatalog(spark: SparkSession,  cat: String): DataFrame = {
    spark.sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  private def readCSV(spark: SparkSession, inputputPath: String, optionsMap: Map[String, String] = Map()) = {
    import spark.implicits._
    spark.read.options(optionsMap).csv(inputputPath).as[ContactRecord]
  }

  private def writeToHbase(inputDf: Dataset[ContactRecord], catalog: String) : Unit =
    inputDf.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "2"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

}
