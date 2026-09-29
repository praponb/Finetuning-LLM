from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

result = (
    lines
    .select("id", "tag")
    .orderBy(F.col("id").desc())
)
