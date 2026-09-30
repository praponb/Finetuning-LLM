from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("element"))
)

result = (
    exploded
    .select("id", "plan")
    .orderBy("plan")
)
