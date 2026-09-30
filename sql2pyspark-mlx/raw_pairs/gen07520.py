from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("tier_value") >= 87, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
    )
    .agg(
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value_2"),
        F.max("tier_value").alias("max_tier_value"),
    )
    .orderBy(F.col("avg_tier_value").desc())
)
