from pyspark.sql import functions as F

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_prices",
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("avg_tier_price_tier", F.when(F.col("avg_tier_price") >= 25, "HIGH").otherwise("LOW"))
    .cube("avg_tier_price_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("entry_price", [0.25, 0.5, 0.75]).alias("entry_price_quartiles"),
        F.grouping("avg_tier_price_tier").alias("grouping_id"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
