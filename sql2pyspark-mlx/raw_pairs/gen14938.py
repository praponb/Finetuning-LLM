from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

cleaned = (
    flattened
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (~F.col("code").eqNullSafe(F.lit("VIP"))))
    .withColumn("code_filled", F.coalesce("code", F.lit("unassigned")))
    .withColumn("code_clean", F.coalesce("code", F.lit("UNKNOWN")))
)

base = (
    cleaned
    .filter(F.col("code").isNotNull())
    .select(
        "id",
        "created_at",
        "code_filled",
        "code_clean",
        F.lower("code").alias("code_lower"),
        F.when(F.col("code").isin(["WELCOME5", "BUNDLE"]), "Tier 1").otherwise("Other").alias("code_group"),
    )
)

cleaned_carts = (
    base
    .filter(F.col("created_at") > F.to_timestamp(F.lit("2025-03-02 01:00:00")))
    .withColumn("created_at_day", F.col("created_at").cast("date"))
)

filtered = (
    cleaned_carts
    .filter(F.col("created_at") > F.lit("2025-03-01").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

summary = (
    filtered
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.when(F.col("days_since_created_at") >= 71, "PREMIUM").otherwise("STANDARD").alias("days_since_created_at_tier"),
    )
    .agg(
        F.min("days_since_created_at").alias("min_days_since_created_at"),
        F.round(F.avg("days_since_created_at"), 2).alias("avg_days_since_created_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.max("created_at").alias("last_seen"),
        F.percentile_approx("days_since_created_at", [0.25, 0.5, 0.75]).alias("days_since_created_at_quartiles"),
    )
)

result = (
    summary
    .select(
        "hour_window",
        "days_since_created_at_tier",
        "min_days_since_created_at",
        "avg_days_since_created_at",
        "last_seen",
    )
    .orderBy(F.col("last_seen").desc(), F.col("hour_window").desc())
)
