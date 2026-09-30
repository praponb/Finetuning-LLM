from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from").between("2025-07-03", "2025-09-18"))
    .select(
        "id",
        "effective_from",
        "tier_limits",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("week", "effective_from").alias("week_start"))
    .agg(
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.percentile_approx("lowest_price", 0.9).alias("lowest_price_p90"),
    )
)
