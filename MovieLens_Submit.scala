import org.apache.spark.sql.SparkSession

val spark=SparkSession.builder().getOrCreate()

val df1=spark.read.option("header","true").option("inferSchema","true").textFile("u.data")
val df2=spark.read.option("header","true").option("inferSchema","true").textFile("u.item")
val df3=spark.read.option("header","true").option("inferSchema","true").textFile("u.user")
val df4=spark.read.option("header","true").option("inferSchema","true").textFile("u.genre")

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

//val dataframe1=df1.toDF()

val dataframeData=df1.withColumn("_tmp",split($"value","\t")).select(
  $"_tmp".getItem(0).cast("Int").as("UserID"),
  $"_tmp".getItem(1).cast("Int").as("MovieID"),
  $"_tmp".getItem(2).cast("Int").as("Rating"),
  $"_tmp".getItem(3).as("UnixTimeStamp")
).drop("_tmp")


val dataframeTotal = dataframeData.withColumn("Total_Ratings",sum('Rating).over())
//dataframeTotal.show(false)

val dataframeDataFinal=dataframeTotal.withColumn("wa_Rating",coalesce(when('Rating >3,'Rating),lit(0))/'Total_Ratings)

val dataframeItem=df2.withColumn("_tmp",split($"value","\\|")).select(
  $"_tmp".getItem(0).cast("Int").as("MovieID"),
  $"_tmp".getItem(1).as("MovieName"),
  $"_tmp".getItem(2).as("ReleaseDate"),
  $"_tmp".getItem(4).as("Genre")
).drop("_tmp")

val dataframeUser=df3.withColumn("_tmp",split($"value","\\|")).select(
  $"_tmp".getItem(0).cast("Int").as("UserID"),
  $"_tmp".getItem(1).cast("Int").as("Age"),
  $"_tmp".getItem(2).as("Gender"),
  $"_tmp".getItem(3).as("Occupation"),
  $"_tmp".getItem(4).cast("Int").as("zipCode")
).drop("_tmp")

val dataframeGenre=df4.withColumn("_tmp",split($"value","\\|")).select(
  $"_tmp".getItem(0).as("Genre"),
  $"_tmp".getItem(1).cast("Int").as("GenreID")
).drop("_tmp")

//val resultDF=dataframeDataFinal.join(dataframeItem,col("dataframeDataFinal.MovieID")===col("dataframeItem.MovieID"),"inner")
val df=dataframeDataFinal.join(dataframeUser,"UserID").join(dataframeItem,"MovieID").select("Occupation","Genre","wa_Rating").sort($"Occupation".asc,$"Genre".asc,$"wa_Rating".desc).show



//dataframeReal.printSchema()
//dataframeReal.take(3)
//dataframeReal.head
//dataframeReal.show()
