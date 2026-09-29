from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", F.explode_outer("discounts").alias("val"))

result = (
    lines
    .select("id", "plan", "val")
    .orderBy(F.col("id").desc_nulls_last())
)
