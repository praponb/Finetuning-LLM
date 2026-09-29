from pyspark.sql import functions as F

parsed = (
    df_price_tiers
    .select(
        "id",
        "currency",
        "tier_limits",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    parsed
    .withColumn("lowest_price_tier", F.when(F.col("lowest_price") >= 27, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["lowest_price_tier", "currency"], ["lowest_price_tier"], []],
        "lowest_price_tier",
        "currency",
    )
    .agg(
        F.avg("lowest_price").alias("avg_lowest_price"),
        F.percentile_approx("lowest_price", [0.25, 0.5, 0.75]).alias("lowest_price_quartiles"),
        F.sum("lowest_price").alias("total_lowest_price"),
        F.round(F.avg("lowest_price"), 2).alias("avg_lowest_price_2"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.grouping("lowest_price_tier").alias("grouping_level"),
    )
    .orderBy("lowest_price_tier")
)
