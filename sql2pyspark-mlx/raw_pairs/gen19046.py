from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("tier_value"))
)

result = (
    flattened
    .select("id", "plan", "tier_value")
    .orderBy(F.col("plan").desc_nulls_first())
)
