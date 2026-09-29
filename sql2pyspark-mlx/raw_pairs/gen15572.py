from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("val"))
)

result = (
    exploded
    .select("id", "effective_from")
    .orderBy("id")
)
