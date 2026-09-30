from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)

result = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "product_id",
        "qty",
        "promo",
    )
    .orderBy(F.col("customer_id").desc())
)
