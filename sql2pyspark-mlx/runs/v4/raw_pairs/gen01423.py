from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2024-04-05").cast("date"))
    .select(
        "*",
        F.floor("sqft").alias("sqft_whole"),
        F.dayofweek("opened_date").alias("opened_date_dayofweek"),
    )
)

filtered = (
    cleaned
    .filter(F.col("city").isin(["Singapore", "New York", "Berlin"]))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_whole",
        "opened_date_dayofweek",
        F.substring("region", 1, 4).alias("region_prefix"),
        F.concat_ws("-", "store_name", "city").alias("store_name_city_key"),
    )
)

valid = (
    filtered
    .filter((F.col("opened_date") >= F.lit("2023-05-06").cast("date")) & (F.col("sqft") > 15852))
    .select(
        "*",
        F.length("region").alias("region_length"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

summary = (
    valid
    .groupBy(
        F.when(F.col("sqft") >= 12281, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
        F.date_trunc("week", "opened_date").alias("week_start"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.size(F.collect_set("city")).alias("num_cities_2"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.sum(F.when(F.col("region") == "North", F.col("sqft_whole")).otherwise(0)).alias("north_sqft_whole"),
    )
    .filter(F.col("singapore_rows") > 1)
)

result = (
    summary
    .select(
        "sqft_tier",
        "week_start",
        "num_cities_2",
        "singapore_rows",
        "north_sqft_whole",
    )
    .orderBy(F.col("week_start").desc_nulls_first())
)
