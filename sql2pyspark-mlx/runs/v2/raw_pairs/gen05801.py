from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter((F.length("store_name") > 7) & (F.col("sqft") <= 5994))
    .withColumn("sqft_filled", F.coalesce("sqft", F.lit(0)))
)

stores_stats = (
    filtered_stores
    .groupBy("city")
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
)

result = (
    stores_stats
    .filter(F.col("stddev_sqft") < 78)
    .select("city", "stddev_sqft")
    .orderBy("city")
)
