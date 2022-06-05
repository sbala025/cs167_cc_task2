package edu.ucr.cs.cs167.sbala025

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object Project {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._

      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "count-by-county" =>
          // TODO count the total number of tweets for each county and display on the screen
          val crimesDF = sparkSession.read.format("csv")

            .option("sep", ",")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(inputFile)

          crimesDF.selectExpr("*", "ST_CreatePoint(x, y) AS geometry")

          /*
          Rename all attributes that include a space to make them easier to deal with and compatible
          with the Parquet format. To do that, use the function withColumnRenamed on the Dataframe.
           */
          val crimesNewDF = crimesDF.withColumnRenamed("ID","ID")
            .withColumnRenamed("Case Number", "CaseNumber")
            .withColumnRenamed("Primary Type", "PrimaryType")
            .withColumnRenamed("Location Description", "LocationDescription")
            .withColumnRenamed("Community Area", "CommunityArea")
            .withColumnRenamed("FBI Code", "FBICode")
            .withColumnRenamed("X Coordinate", "XCoordinate")
            .withColumnRenamed("Y Coordinate", "YCoordinate")
            .withColumnRenamed("Updated On", "UpdatedOn")
          //crimesNewDF.show()
          //crimesNewDF.printSchema()


          //Convert the resulting Dataframe to a SpatialRDD to prepare for the next step.
          val crimesRDD: SpatialRDD = crimesNewDF.selectExpr("*", "ST_CreatePoint(x, y) AS geometry").toSpatialRDD

          //Load the ZIP Code dataset using Beast.
          val zipFile: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")

          //Run a spatial join query to find the ZIP code of each crime.
          val zipEachCrime: RDD[(IFeature, IFeature)] = crimesRDD.spatialJoin(zipFile)

          //Use the attribute ZCTA5CE10 from the ZIP code to introduce a new attribute ZIPCode in the crime.
          //Convert the result back to a Dataframe.
          val newZip: DataFrame = zipEachCrime.map({ case (data, zip) => Feature.append(data, zip.getAs[String]("ZCTA5CE10"), "ZIPCode") })
            .toDataFrame(sparkSession)
          val dropZip = newZip.drop("geometry")

          //show
           //dropZip.printSchema()
          // dropZip.show()
          dropZip.write.mode(SaveMode.Overwrite).parquet("Chicago_Crimes_ZIP")

          val t2 = System.nanoTime()
          if (validOperation)
            println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
          else
            Console.err.println(s"Invalid operation '$operation'")
        case "choropleth-map" =>
          val outputFile: String = args(2)
          val crimesmapDF = sparkSession.read.parquet("Chicago_Crimes_ZIP").createOrReplaceTempView("crimes")
          sparkSession.sql(s"""
            SELECT ZIPCode, count(*) AS temp
            FROM crimes
            GROUP BY ZIPCode
            """)
            .createOrReplaceTempView("crimeZIP_counts")

          sparkContext.shapefile("tl_2018_us_zcta510.zip")
            .toDataFrame(sparkSession)
            .createOrReplaceTempView("counties")

          sparkSession.sql(
            s"""
            SELECT ZIPCode, g, temp
            FROM crimeZIP_counts, counties
            WHERE ZIPCode = ZCTA5CE10
            """).toSpatialRDD
            .coalesce(1)
            .saveAsShapefile(outputFile)
      } }finally {
      sparkSession.stop()
    }
  }
}