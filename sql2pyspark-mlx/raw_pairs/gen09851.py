from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .groupBy(
        "currency",
        F.when(F.col("avg_tier_price") >= 25, "HIGH").otherwise("LOW").alias("avg_tier_price_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.round(F.sum("avg_tier_price") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("avg_tier_price_per_starter"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
    )
    .filter(F.col("avg_tier_price_per_starter") > 0)
    .orderBy(F.col("eur_count").asc_nulls_last())
)
