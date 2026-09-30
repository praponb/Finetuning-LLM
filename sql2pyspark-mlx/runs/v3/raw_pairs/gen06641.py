from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left_semi")
    .select(
        F.col("orders.id"),
        F.col("orders.customer_id"),
        F.col("orders.shipping_fee"),
    )
    .orderBy(F.col("customer_id").desc())
)
