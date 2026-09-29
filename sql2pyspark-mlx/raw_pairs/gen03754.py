from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .filter(F.col("currency") == "THB")
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("avg_tier_price") >= 26, "LARGE").otherwise("SMALL").alias("avg_tier_price_tier"),
    )
    .agg(
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("distinct_currencies").asc_nulls_last())
)
