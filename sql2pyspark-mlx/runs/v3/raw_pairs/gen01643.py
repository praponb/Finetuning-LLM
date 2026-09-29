from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("region")

result = (
    df_stores
    .filter(~F.col("store_name").like("%Airpor%"))
    .groupBy("region")
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("cumulative_sqft", F.sum("total_sqft").over(w))
    .orderBy(F.col("cumulative_sqft").desc(), F.col("total_sqft").desc())
)
