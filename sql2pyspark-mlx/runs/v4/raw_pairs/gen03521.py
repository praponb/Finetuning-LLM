from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_prices").alias("tier_value"))
)

valid = (
    flattened
    .filter((F.col("effective_from") > F.lit("2024-06-15").cast("date")) & (F.col("tier_value") <= 74))
    .select(
        "*",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

valid_price_tiers = (
    valid
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-06-12")))
    .select(
        "*",
        F.coalesce("days_since_effective_from", F.lit(0)).alias("days_since_effective_from_filled"),
    )
)

summary = (
    valid_price_tiers
    .groupBy(
        F.when(F.col("tier_value") >= 87, "PREMIUM").otherwise("STANDARD").alias("tier_value_tier"),
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("days_since_effective_from"), 2).alias("avg_days_since_effective_from"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles_2"),
    )
    .filter(F.col("num_rows") > 1)
)

result = (
    summary
    .select(
        "tier_value_tier",
        "year_start",
        "distinct_ids",
        "avg_days_since_effective_from",
        "distinct_ids_2",
        "tier_value_quartiles",
    )
)
