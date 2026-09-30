from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("tier_value"))

result = (
    exploded
    .groupBy(
        F.when(F.col("tier_value") >= 78, "HIGH").otherwise("LOW").alias("tier_value_tier"),
    )
    .agg(
        F.sum("tier_value").alias("total_tier_value"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("tier_value").alias("total_tier_value_2"),
    )
    .orderBy("total_tier_value")
)
