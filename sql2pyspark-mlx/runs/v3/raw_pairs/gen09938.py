from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_sqft").desc())

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-02-01")
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("rnk", F.dense_rank().over(w))
)
