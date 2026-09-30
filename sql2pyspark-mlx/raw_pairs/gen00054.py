from pyspark.sql import functions as F

result = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select("id", "customer_id", "pos", F.col("item.product_id"), F.col("item.promo"))
)
