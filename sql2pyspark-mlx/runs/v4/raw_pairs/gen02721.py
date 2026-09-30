from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2024-05-23"))) & F.col("channel").isNotNull() & F.col("channel").isin(["store", "web"]))
    .withColumn("ends_on_day", F.dayofmonth("ends_on"))
)

cleaned_promotions = (
    cleaned
    .filter((F.col("starts_on") >= F.lit("2023-07-31").cast("date")) & F.col("channel").isNotNull())
    .withColumn("days_ends_on_to_starts_on", F.datediff("starts_on", "ends_on"))
    .withColumn("discount_pct_rounded", F.round("discount_pct", 0))
)

valid = (
    cleaned_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "ends_on_day",
        "days_ends_on_to_starts_on",
        "discount_pct_rounded",
        F.datediff("ends_on", "starts_on").alias("days_starts_on_to_ends_on"),
        F.floor("discount_pct").alias("discount_pct_whole"),
    )
)

result = (
    valid
    .withColumn("ends_on_day_tier", F.when(F.col("ends_on_day") >= 87, "PREMIUM").otherwise("STANDARD"))
    .rollup("ends_on_day_tier")
    .agg(
        F.count_if(F.col("channel") == "mobile").alias("mobile_events"),
        F.sum(F.when(F.col("channel") == "partner", F.col("days_ends_on_to_starts_on"))).alias("partner_total_days_ends_on_to_starts_on"),
        F.percentile_approx("discount_pct_rounded", 0.9).alias("discount_pct_rounded_p90"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("partner_total_days_ends_on_to_starts_on").desc())
)
