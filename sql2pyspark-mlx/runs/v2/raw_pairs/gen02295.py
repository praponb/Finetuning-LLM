from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_quarter")

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2025-07-01")
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.sum("units").alias("total_units"))
    .withColumn("cumulative_units", F.sum("total_units").over(w))
)
