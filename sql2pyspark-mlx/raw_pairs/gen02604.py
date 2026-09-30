from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("tier_value"))
)

result = (
    lines
    .select("id")
    .orderBy(F.col("id").desc_nulls_last())
)
