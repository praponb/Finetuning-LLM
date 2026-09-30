from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("discounts").alias("tier_value"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("tier_value") >= 73, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.avg("tier_value").alias("avg_tier_value"),
        F.percentile_approx("tier_value", [0.25, 0.5, 0.75]).alias("tier_value_quartiles"),
    )
    .filter(F.col("avg_tier_value") >= 0)
    .orderBy(F.col("tier_value_tier").desc())
)
