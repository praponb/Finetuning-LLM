from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("sqft") == 15451)

stores_stats = (
    valid_stores
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sum("sqft").alias("total_sqft"),
    )
)

result = (
    stores_stats
    .filter(F.col("total_sqft") >= 52000)
    .select("city", "stddev_sqft", "total_sqft")
    .orderBy("city")
    .limit(10)
)
