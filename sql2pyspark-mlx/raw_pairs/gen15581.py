from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

result = (
    exploded
    .select("id")
    .orderBy(F.col("id").asc_nulls_last())
)
