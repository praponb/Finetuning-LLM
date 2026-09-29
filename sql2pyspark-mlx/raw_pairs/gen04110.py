from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("val"))
)

result = (
    exploded
    .select("id", "plan", "effective_from")
    .orderBy(F.col("id").desc_nulls_first(), "effective_from")
)
