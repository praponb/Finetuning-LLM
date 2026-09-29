from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", F.explode_outer("tier_limits").alias("val"))

result = (
    flattened
    .select("val")
    .orderBy(F.col("val").desc_nulls_first())
)
