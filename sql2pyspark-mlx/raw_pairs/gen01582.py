from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .groupBy("currency")
    .agg(
        F.avg(F.when(F.col("plan") == "enterprise", F.col("lowest_price"))).alias("enterprise_avg_lowest_price"),
        F.round(F.sum("lowest_price") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("lowest_price_per_thb"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("entry_price")).otherwise(0)).alias("eur_entry_price"),
        F.sum(F.when(F.col("plan") == "basic", F.col("lowest_price"))).alias("basic_total_lowest_price"),
    )
    .orderBy(F.col("basic_total_lowest_price").desc())
)
