from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

lines = exploded.select("created_at", "k", "value", F.explode("coupon_codes").alias("code"))

cleaned = (
    lines
    .filter((F.col("created_at") > F.lit("2025-03-01 11:00:00").cast("timestamp")) & (F.col("k") != "device"))
    .withColumn("k_upper", F.upper("k"))
    .withColumn("code_filled", F.coalesce("code", F.lit("N/A")))
)

cleaned_carts = (
    cleaned
    .filter(F.col("created_at") > F.lit("2025-03-01 12:00:00").cast("timestamp"))
    .select(
        "created_at",
        "k",
        "code",
        "k_upper",
        "code_filled",
        F.lower("code").alias("code_lower"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled"),
    )
)

result = (
    cleaned_carts
    .rollup(
        "k",
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.count_if(F.col("code") == "BUNDLE").alias("bundle_events"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
    .orderBy(F.col("k").desc_nulls_first())
)
