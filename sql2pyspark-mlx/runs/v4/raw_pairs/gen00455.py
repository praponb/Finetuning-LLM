from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .withColumn("activity_month", F.date_trunc("MONTH", "effective_from"))
    .withColumn("avg_tier_price_tier", F.when(F.col("avg_tier_price") >= 29, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "effective_from"))
    .rollup("activity_month", "avg_tier_price_tier", "week_start")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("avg_tier_price_tier").desc())
)
