from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

result = (
    exploded
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", F.col("sum_order_discount"))
    .orderBy(F.col("id").desc_nulls_first())
)
