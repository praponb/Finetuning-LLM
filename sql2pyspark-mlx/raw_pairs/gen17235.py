from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_limits").alias("tier_value"))
)

result = (
    flattened
    .groupBy(
        F.when(F.col("tier_value") >= 85, "HIGH").otherwise("LOW").alias("tier_value_tier"),
    )
    .agg(
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles_2"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
    )
    .orderBy(F.col("avg_tier_value").desc_nulls_last())
)
