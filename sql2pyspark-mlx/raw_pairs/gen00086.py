from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("element"))
)

result = (
    exploded
    .select("id", "effective_from", "element")
    .orderBy(F.col("id").desc_nulls_first())
)
