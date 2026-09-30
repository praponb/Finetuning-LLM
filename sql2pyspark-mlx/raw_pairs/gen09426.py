from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("tier_value"))
)

result = (
    flattened
    .select("id", "effective_from", "tier_value")
    .orderBy(F.col("tier_value").asc_nulls_first(), "effective_from")
)
