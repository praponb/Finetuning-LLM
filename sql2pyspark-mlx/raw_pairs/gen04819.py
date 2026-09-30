from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .cube(
        F.date_trunc("WEEK", "effective_from").alias("week_start"),
        F.when(F.col("avg_tier_price") >= 30, "PREMIUM").otherwise("STANDARD").alias("avg_tier_price_tier"),
    )
    .agg(
        F.percentile_approx("lowest_price", 0.5).alias("lowest_price_p50"),
        F.count("*").alias("cnt"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.count("*").alias("event_count"),
    )
    .orderBy("cnt")
)
