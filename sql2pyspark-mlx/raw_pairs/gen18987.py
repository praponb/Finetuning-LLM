from pyspark.sql import functions as F

result = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("item.product_id"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)
