from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("entry", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)
