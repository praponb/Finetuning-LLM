from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

result = lines.select("id", "val")
