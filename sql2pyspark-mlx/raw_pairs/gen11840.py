from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "rec"))
    .select(
        "id",
        "coupon_codes",
        "pos",
        F.col("rec.product_id"),
        F.col("rec.qty"),
        F.col("rec.price"),
    )
)

result = (
    exploded
    .select("id", "coupon_codes", "pos", "qty")
    .orderBy("id")
)
