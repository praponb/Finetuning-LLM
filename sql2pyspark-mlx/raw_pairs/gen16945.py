from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

result = (
    flattened
    .select("id", "effective_from", "val")
    .orderBy("effective_from")
)
