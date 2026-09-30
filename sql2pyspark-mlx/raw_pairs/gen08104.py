from pyspark.sql import functions as F

discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

result = (
    exploded
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "tag",
        F.when(F.col("tag").rlike("^(VIP|SPRING10)$"), "CORE").otherwise("PRIORITY").alias("tag_zone"),
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("tag_zone").desc_nulls_last())
)
