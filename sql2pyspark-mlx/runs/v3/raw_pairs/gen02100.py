from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_cost").desc())

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2024-07-01")
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(
        F.sum("cost").alias("total_cost"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("cnt").desc())
)
