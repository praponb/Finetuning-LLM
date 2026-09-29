from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_limits",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    parsed
    .groupBy("plan")
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.sum(F.when(F.col("plan") == "starter", F.col("avg_tier_price")).otherwise(0)).alias("starter_avg_tier_price"),
        F.count("*").alias("row_count"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles_2"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
)
