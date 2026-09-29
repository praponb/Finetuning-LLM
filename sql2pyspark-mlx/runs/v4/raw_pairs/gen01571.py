from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("city")

result = (
    df_stores
    .filter(F.col("sqft") <= 11641)
    .groupBy("city")
    .agg(F.sum("sqft").alias("total_sqft"))
    .withColumn("cumulative_sqft", F.sum("total_sqft").over(w))
    .orderBy("total_sqft")
)
