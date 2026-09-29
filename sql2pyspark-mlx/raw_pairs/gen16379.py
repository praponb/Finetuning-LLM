from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

result = (
    exploded
    .select("id", "tier_value")
    .orderBy(F.col("id").asc_nulls_last())
)
