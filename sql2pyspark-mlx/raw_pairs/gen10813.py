from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_limits").alias("val"))
)

result = (
    lines
    .select("effective_from", "currency", "val")
    .orderBy(F.col("val").asc_nulls_first())
)
