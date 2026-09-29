from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan") == "starter")
    .select(
        "id",
        "plan",
        "tier_limits",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        "plan",
        F.when(F.col("entry_price") >= 28, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
        F.when(F.col("lowest_price") >= 26, "LARGE").otherwise("SMALL").alias("lowest_price_tier"),
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.count("*").alias("event_count"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("avg_tier_price")).otherwise(0)).alias("enterprise_avg_tier_price"),
    )
)
