from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "coupon_codes", F.explode_outer("attributes").alias("key", "value"))
)

result = (
    lines
    .select("id", "coupon_codes", "key")
    .orderBy(F.col("key").desc())
)
