from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .groupBy("currency")
    .agg(
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.percentile_approx("lowest_price", 0.95).alias("lowest_price_p95"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
    )
    .filter(F.col("lowest_price_p95") >= 0)
)
