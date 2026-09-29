from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_prices").alias("tier_value"))
)

result = (
    exploded
    .select("tier_value")
    .orderBy(F.col("tier_value").asc_nulls_first())
)
