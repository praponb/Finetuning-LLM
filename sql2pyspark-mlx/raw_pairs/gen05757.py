from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("tier_value"))

result = (
    exploded
    .select("id", "tier_value")
    .orderBy(F.col("tier_value").desc())
)
