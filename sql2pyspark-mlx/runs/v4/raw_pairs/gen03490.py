from pyspark.sql import functions as F

result = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select("id", "coupon_codes", "pos", F.col("line.product_id"), F.col("line.qty"))
)
