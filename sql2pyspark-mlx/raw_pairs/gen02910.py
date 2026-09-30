from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan").isin(["enterprise", "basic", "starter"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_prices",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("plan") == "pro", F.col("avg_tier_price")).otherwise(0)).alias("pro_avg_tier_price"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
        F.round(F.sum("lowest_price") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("lowest_price_per_starter"),
    )
)
