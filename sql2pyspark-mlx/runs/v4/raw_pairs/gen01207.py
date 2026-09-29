from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("entry_price") >= 29, "PREMIUM").otherwise("STANDARD").alias("entry_price_tier"),
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
    )
    .agg(
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.sum("entry_price").alias("total_entry_price"),
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles_2"),
        F.min("entry_price").alias("min_entry_price"),
        F.min("entry_price").alias("min_entry_price_2"),
    )
    .filter(F.col("total_entry_price") < 0)
    .orderBy(F.col("min_entry_price").desc())
)
