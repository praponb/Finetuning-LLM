from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .filter(F.col("plan") == "starter")
    .select(
        "id",
        "plan",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .groupBy(
        "plan",
        F.when(F.col("entry_price") >= 34, "LARGE").otherwise("SMALL").alias("entry_price_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price"),
        F.round(F.avg("avg_tier_price"), 2).alias("avg_avg_tier_price_2"),
        F.round(F.sum("entry_price") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("entry_price_per_pro"),
    )
)
