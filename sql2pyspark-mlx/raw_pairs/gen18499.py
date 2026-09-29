from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .cube(
        F.when(F.col("entry_price") >= 30, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
    )
    .agg(
        F.percentile_approx("avg_tier_price", 0.9).alias("avg_tier_price_p90"),
        F.count("*").alias("cnt"),
    )
)
