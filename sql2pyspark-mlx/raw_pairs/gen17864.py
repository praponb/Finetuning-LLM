from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("plan") == "starter", F.col("avg_tier_price")).otherwise(0)).alias("starter_avg_tier_price"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("avg_tier_price"))).alias("eur_total_avg_tier_price"),
        F.sum(F.when(F.col("currency") == "THB", F.col("avg_tier_price")).otherwise(0)).alias("thb_avg_tier_price"),
    )
    .orderBy(F.col("starter_avg_tier_price").asc_nulls_last())
)
