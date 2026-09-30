from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("k", "v"),
    )
)

result = (
    flattened
    .select("id", "customer_id", "coupon_codes", "k", "v")
    .orderBy(F.col("id").desc(), "customer_id")
)
