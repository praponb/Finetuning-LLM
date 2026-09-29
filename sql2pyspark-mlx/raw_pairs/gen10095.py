from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    flattened
    .select("customer_id", "coupon_codes", "product_id", "price", "promo")
    .orderBy(F.col("price").asc_nulls_first(), "product_id")
)
