from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "right")
    .filter(F.col("orders.customer_id").isNull() & (F.col("order_items.quantity") > 7))
    .select(
        F.col("order_items.id"),
        F.col("order_items.product_id"),
        F.col("orders.amount"),
        F.col("orders.discount"),
        F.col("orders.order_date"),
    )
)
