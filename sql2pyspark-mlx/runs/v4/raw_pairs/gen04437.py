from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "tier_limits",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("lowest_price") >= 28, "HIGH").otherwise("LOW").alias("lowest_price_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("event_count"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.percentile_approx("lowest_price", 0.9).alias("lowest_price_p90"),
        F.max("avg_tier_price").alias("max_avg_tier_price"),
    )
    .filter(F.col("num_rows") >= 1)
    .orderBy(F.col("lowest_price_p90").asc_nulls_last())
)
