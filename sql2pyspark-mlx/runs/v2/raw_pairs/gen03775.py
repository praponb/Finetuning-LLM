from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units_bucket")

result = (
    df_store_sales
    .filter(F.col("units") < 34)
    .groupBy(
        F.when(F.col("units") >= 40, "high").when(F.col("units") >= 9, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.sum("units").alias("total_units"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_units", F.sum("total_units").over(w))
)
