from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_prices",
        "discounts",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    parsed
    .withColumn("lowest_price_tier", F.when(F.col("lowest_price") >= 35, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["currency", "lowest_price_tier"], ["currency"]],
        "currency",
        "lowest_price_tier",
    )
    .agg(
        F.sum(F.when(F.col("currency") == "THB", F.col("lowest_price"))).alias("thb_total_lowest_price"),
        F.avg(F.when(F.col("currency") == "THB", F.col("lowest_price"))).alias("thb_avg_lowest_price"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.grouping("currency").alias("grouping_id"),
    )
)
