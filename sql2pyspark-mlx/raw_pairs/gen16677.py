from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

result = (
    lines
    .select("effective_from", "val")
    .orderBy("val", "effective_from")
)
