from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_prices").alias("tier_value"))
)

result = (
    flattened
    .select("id", "tier_value")
    .orderBy(F.col("id").desc_nulls_last())
)
