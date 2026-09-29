from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.opened_date")), "left_anti")
)

kept_keys = (
    not_flagged
    .select("region")
    .intersectAll(
        df_stores
        .filter(F.col("region") == "East")
        .select("region")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter((F.col("opened_date") >= F.lit("2023-05-20").cast("date")) & (F.col("opened_date") <= "2023-11-16"))
    .select(
        "*",
        F.ceil("sqft").alias("sqft_whole"),
        F.regexp_replace("store_name", " ", "").alias("store_name_slug"),
    )
)

filtered = (
    valid
    .filter(F.col("opened_date") >= "2024-10-12")
    .select("*", (F.col("sqft_whole") + F.col("sqft")).alias("sqft_whole_plus_sqft"))
)

remaining_keys = (
    filtered
    .select("region", "city")
    .intersect(
        df_stores
        .filter(F.col("region") == "West")
        .select("region", "city")
    )
)

kept_rows_stores = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows_stores
    .groupBy("city")
    .agg(
        F.percentile_approx("sqft_whole", 0.95).alias("sqft_whole_p95"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("sqft_whole_plus_sqft")).otherwise(0)).alias("berlin_sqft_whole_plus_sqft"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.size(F.collect_set("region")).alias("num_regions_2"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
    )
)
