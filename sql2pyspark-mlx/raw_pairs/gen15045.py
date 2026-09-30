from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("discount").alias("max_order_discount"))

flattened = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("entry.product_id"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

result = (
    flattened
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "price", "promo", F.col("max_order_discount"))
)
