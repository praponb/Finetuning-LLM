from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .groupBy(
        "plan",
        F.when(F.col("lowest_price") >= 27, "LARGE").otherwise("SMALL").alias("lowest_price_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.count("*").alias("cnt"),
        F.avg("lowest_price").alias("avg_lowest_price_2"),
    )
    .orderBy("avg_lowest_price")
)
