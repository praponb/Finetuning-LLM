from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .withColumn("tier_value_tier", F.when(F.col("tier_value") >= 62, "HIGH").otherwise("LOW"))
    .rollup("tier_value_tier")
    .agg(
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("tier_value_per_pro"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.grouping_id().alias("gid"),
    )
)
