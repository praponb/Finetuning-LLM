from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("lowest_price") >= 34, "LARGE").otherwise("SMALL").alias("lowest_price_tier"),
        F.when(F.col("avg_tier_price") >= 31, "HIGH").otherwise("LOW").alias("avg_tier_price_tier"),
    )
    .agg(
        F.round(F.sum("lowest_price") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("lowest_price_per_eur"),
        F.min("lowest_price").alias("min_lowest_price"),
        F.percentile_approx("avg_tier_price", 0.95).alias("avg_tier_price_p95"),
    )
)
