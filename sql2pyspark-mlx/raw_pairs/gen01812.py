from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

result = exploded.select("id")
