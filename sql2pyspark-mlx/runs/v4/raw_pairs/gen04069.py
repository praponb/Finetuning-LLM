from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "tier_limits",
        "tier_prices",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("entry_price") >= 28, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
    )
    .agg(
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.percentile_approx("entry_price", 0.5).alias("entry_price_p50"),
        F.count("*").alias("row_count"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price"),
        F.max("entry_price").alias("max_entry_price"),
        F.round(F.avg("entry_price"), 2).alias("avg_entry_price_2"),
    )
    .filter(F.col("max_entry_price") > 0)
)
