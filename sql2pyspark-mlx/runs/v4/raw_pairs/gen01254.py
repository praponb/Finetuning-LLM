from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("city") == "Bangkok")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        F.coalesce("store_name", F.lit("N/A")).alias("store_name_filled"),
    )
)

cleaned = (
    valid
    .filter(~F.col("city").isin(["New York", "Tokyo"]))
    .select(
        "*",
        F.upper("region").alias("region_upper"),
        F.substring("store_name", 1, 3).alias("store_name_prefix"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.discount") <= 0.35), "left_semi")
)

result = (
    not_flagged
    .groupBy("city")
    .agg(
        F.count_if(F.col("region") == "West").alias("west_events"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("west_events") >= 1)
)
