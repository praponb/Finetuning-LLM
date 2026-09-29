from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

result = (
    exploded
    .select("plan", "currency", "val")
    .orderBy(F.col("val").desc(), F.col("currency").desc())
)
