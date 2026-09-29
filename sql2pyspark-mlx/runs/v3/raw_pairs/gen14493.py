from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter(F.lower("store_name").like("%ive%"))
    .withColumn("region_title", F.initcap(F.trim("region")))
)

city_summary = (
    valid_stores
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
)

result = city_summary.select("city", "stddev_sqft", "tokyo_count")
