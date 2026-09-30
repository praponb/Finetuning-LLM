from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", F.explode("tier_prices").alias("element"))

result = (
    exploded
    .select("id", "element")
    .orderBy(F.col("id").desc())
)
