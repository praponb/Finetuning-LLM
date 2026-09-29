from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("element"))

result = (
    exploded
    .select("id", "element")
    .orderBy(F.col("element").asc_nulls_first())
)
