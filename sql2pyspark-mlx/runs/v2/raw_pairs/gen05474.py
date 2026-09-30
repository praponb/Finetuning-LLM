from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_quarter")

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("cumulative_sqft", F.sum("total_sqft").over(w))
    .orderBy(F.col("opened_quarter").desc())
)
