from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        (F.col("pos") + 1).alias("position"),
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
    )
)

result = (
    exploded
    .select("id", "customer_id", "coupon_codes", "position", "product_id", "price")
    .orderBy("price")
)
