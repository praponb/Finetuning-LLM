from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "effective_from", F.explode("tier_prices").alias("val"))

result = (
    lines
    .select("effective_from", "val")
    .orderBy(F.col("effective_from").desc())
)
