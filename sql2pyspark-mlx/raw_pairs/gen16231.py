from pyspark.sql import functions as F

result = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)
