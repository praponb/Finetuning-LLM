from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "key",
        "attr_value",
        F.when(F.col("key").rlike("^(gift|device)$"), "GROUP_B").when(F.col("key").rlike("^(utm_source|weight_kg)$"), "CORE").otherwise("EXTENDED").alias("key_zone"),
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("id").asc_nulls_last())
)
