from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(~F.col("plan").isin(["pro", "enterprise"]))
    .select(
        "id",
        "plan",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
)
