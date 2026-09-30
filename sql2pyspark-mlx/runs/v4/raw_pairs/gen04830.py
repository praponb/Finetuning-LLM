from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("currency") == "EUR")
    .select(
        "id",
        "currency",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("entry_price") >= 27, "LARGE").otherwise("SMALL").alias("entry_price_tier"),
    )
    .agg(
        F.round(F.sum("lowest_price") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("lowest_price_per_eur"),
        F.percentile_approx("lowest_price", 0.9).alias("lowest_price_p90"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
    )
)
