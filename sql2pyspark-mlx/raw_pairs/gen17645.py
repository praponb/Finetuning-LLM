from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("city") == "Bangkok"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.initcap(F.trim("region")).alias("region_title"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

result = (
    cleaned
    .groupBy("region")
    .pivot("city", ["Bangkok", "Chiang Mai", "Berlin"])
    .agg(F.max("sqft").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "region",
        F.col("Bangkok_value_sum").alias("bangkok_value_sum"),
        F.col("Bangkok_item_count").alias("bangkok_item_count"),
        F.col("Chiang Mai_value_sum").alias("chiang_mai_value_sum"),
        F.col("Chiang Mai_item_count").alias("chiang_mai_item_count"),
        F.col("Berlin_value_sum").alias("berlin_value_sum"),
        F.col("Berlin_item_count").alias("berlin_item_count"),
    )
)
