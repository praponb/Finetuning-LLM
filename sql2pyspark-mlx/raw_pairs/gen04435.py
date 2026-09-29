from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("element"))
)

result = (
    flattened
    .select("effective_from")
    .orderBy("effective_from")
)
