from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

result = (
    lines
    .select("id", "customer_id")
    .orderBy(F.col("id").desc_nulls_first())
)
