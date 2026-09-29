from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

matched_keys = (
    lines
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isin([12, 21, 25]))
        .select("id")
    )
)

reconciled = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter((F.col("created_at") > F.lit("2025-03-01 16:00:00").cast("timestamp")) & (~F.col("attr_key").eqNullSafe(F.lit("device"))))
    .withColumn("attr_key_title", F.initcap(F.trim("attr_key")))
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("UNKNOWN")))
)

valid = (
    cleaned
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("attr_key").isin(["utm_source", "gift", "weight_kg"]))
    .select(
        "created_at",
        "coupon_codes",
        "attr_key",
        "attr_value",
        "attr_key_title",
        F.hour("created_at").alias("created_at_hour"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled_2"),
    )
)

cleaned_carts = (
    valid
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & (~F.col("attr_key").eqNullSafe(F.lit("utm_source"))))
    .select(
        "created_at",
        "attr_value",
        "attr_key_title",
        "created_at_hour",
        "attr_key_filled_2",
        F.coalesce("created_at_hour", F.lit(0)).alias("created_at_hour_filled"),
    )
)

result = (
    cleaned_carts
    .groupBy(
        F.date_trunc("day", "created_at").alias("activity_day"),
        F.date_trunc("hour", "created_at").alias("hour_window"),
    )
    .agg(
        F.max("created_at").alias("last_seen"),
        F.percentile_approx("created_at_hour_filled", [0.25, 0.5, 0.75]).alias("created_at_hour_filled_quartiles"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.min("created_at").alias("first_seen"),
        F.min("created_at").alias("first_seen_2"),
        F.max("created_at").alias("ended_at"),
    )
    .filter(F.col("span_minutes") >= 0)
    .orderBy(F.col("activity_day").desc())
)
