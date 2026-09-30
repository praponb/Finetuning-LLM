from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)
