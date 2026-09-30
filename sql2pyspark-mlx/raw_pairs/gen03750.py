from pyspark.sql import functions as F

lines = df_price_tiers.select("id", F.explode("tier_limits").alias("tier_value"))

result = (
    lines
    .select("id", "tier_value")
    .orderBy(F.col("id").desc_nulls_first(), "tier_value")
)
