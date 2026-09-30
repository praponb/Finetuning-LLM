from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("city")

result = (
    df_stores
    .filter(F.col("opened_date") < "2025-05-16")
    .groupBy("city")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_sqft", F.sum("total_sqft").over(w))
    .orderBy(F.col("cumulative_sqft").desc(), "city")
)
