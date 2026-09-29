from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("rec", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("rec.product_id"),
        F.col("rec.price"),
        F.col("rec.promo"),
    )
)
