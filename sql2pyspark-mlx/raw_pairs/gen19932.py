from pyspark.sql import functions as F

result = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "pos",
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)
