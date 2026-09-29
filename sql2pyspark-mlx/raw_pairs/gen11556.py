from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.amount") <= 810)
    .select(
        F.col("order_items.unit_price"),
        F.col("order_items.order_id"),
        F.col("orders.customer_id"),
        F.col("orders.discount"),
        F.col("orders.status"),
    )
)
