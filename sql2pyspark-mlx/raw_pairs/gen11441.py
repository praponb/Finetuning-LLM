from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("entry_price") >= 27, "HIGH").otherwise("LOW").alias("entry_price_tier"),
    )
    .agg(
        F.avg("entry_price").alias("avg_entry_price"),
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("avg_entry_price").asc_nulls_first())
)
