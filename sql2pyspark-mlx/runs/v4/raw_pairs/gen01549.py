from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

result = exploded.select("customer_id", "created_at")
