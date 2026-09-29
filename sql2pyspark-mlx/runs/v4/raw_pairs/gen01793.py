from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("avg_tier_price") >= 28, "HIGH").otherwise("LOW").alias("avg_tier_price_tier"),
    )
    .agg(
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.percentile_approx("avg_tier_price", 0.9).alias("avg_tier_price_p90"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
