from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter((F.col("opened_date") >= "2024-04-21") & (F.col("store_name").isNull() | (F.col("sqft") == 7568)))
    .withColumn("city_store_name_key", F.concat_ws("-", "city", "store_name"))
)

agg = (
    valid_stores
    .groupBy("city", "region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
    )
)

result = (
    agg
    .filter(F.col("sqft_count") < 5)
    .orderBy("pct_west", "city", "region")
    .limit(10)
)
