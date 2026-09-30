from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "effective_from", F.explode("tier_prices").alias("val"))

result = (
    exploded
    .select("val")
    .orderBy(F.col("val").desc())
)
