from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("avg_tier_price") >= 34, "HIGH").otherwise("LOW").alias("avg_tier_price_tier"),
        F.when(F.col("entry_price") >= 28, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
    )
    .agg(
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
        F.percentile_approx("avg_tier_price", [0.25, 0.5, 0.75]).alias("avg_tier_price_quartiles"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("entry_price_tier").asc_nulls_first())
)
