from pyspark.sql import functions as F

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .withColumn("lowest_price_tier", F.when(F.col("lowest_price") >= 27, "HIGH").otherwise("LOW"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "effective_from"))
    .groupingSets(
        [["currency", "lowest_price_tier", "quarter_start"], ["currency", "lowest_price_tier"], ["currency"]],
        "currency",
        "lowest_price_tier",
        "quarter_start",
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.avg("lowest_price").alias("avg_lowest_price"),
        F.count("*").alias("row_count"),
        F.grouping("currency").alias("grouping_id"),
    )
    .orderBy(F.col("avg_lowest_price").desc())
)
