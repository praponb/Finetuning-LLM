from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(~F.col("plan").isin(["starter", "enterprise"]))
    .select(
        "id",
        "plan",
        "tier_limits",
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .groupBy("plan")
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.sum("avg_tier_price").alias("total_avg_tier_price"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("total_avg_tier_price").desc())
)
