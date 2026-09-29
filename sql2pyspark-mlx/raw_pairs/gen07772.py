from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("entry_price_tier", F.when(F.col("entry_price") >= 27, "HIGH").otherwise("LOW"))
    .withColumn("lowest_price_tier", F.when(F.col("lowest_price") >= 34, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["entry_price_tier", "lowest_price_tier"], ["entry_price_tier"]],
        "entry_price_tier",
        "lowest_price_tier",
    )
    .agg(
        F.percentile_approx("lowest_price", 0.95).alias("lowest_price_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("lowest_price_p95").desc_nulls_first())
)
