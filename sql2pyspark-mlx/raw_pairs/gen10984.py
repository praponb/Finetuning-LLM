from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .select("id", "customer_id", "coupon_codes", "qty", "promo")
    .orderBy(F.col("customer_id").desc_nulls_first())
)
