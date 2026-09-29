from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        "discounts",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
        F.when(F.col("avg_tier_price") >= 24, "PREMIUM").otherwise("STANDARD").alias("avg_tier_price_tier"),
    )
    .agg(
        F.percentile_approx("avg_tier_price", 0.5).alias("avg_tier_price_p50"),
        F.sum(F.when(F.col("currency") == "USD", F.col("lowest_price"))).alias("usd_total_lowest_price"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("avg_tier_price_tier").asc_nulls_first())
)
