from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .filter(F.year("effective_from") == 2025)
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("MONTH", "effective_from").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "starter", F.col("lowest_price")).otherwise(0)).alias("starter_lowest_price"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .orderBy(F.col("avg_lowest_price").desc())
)
