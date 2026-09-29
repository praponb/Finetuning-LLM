from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("effective_from") >= "2023-06-01")
    .select(
        "id",
        "effective_from",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("avg_tier_price") >= 33, "PREMIUM").otherwise("STANDARD").alias("avg_tier_price_tier"),
    )
    .agg(
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.min("lowest_price").alias("min_lowest_price"),
        F.percentile_approx("entry_price", 0.95).alias("entry_price_p95"),
    )
    .orderBy(F.col("avg_avg_tier_price").desc())
)
