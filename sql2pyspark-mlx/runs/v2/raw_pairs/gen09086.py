from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_revenue").desc())

result = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 6]))
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy("rnk")
)
