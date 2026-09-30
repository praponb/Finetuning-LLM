from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("tier_value"))

result = (
    flattened
    .select("id", "plan", "tier_value")
    .orderBy(F.col("tier_value").desc())
)
