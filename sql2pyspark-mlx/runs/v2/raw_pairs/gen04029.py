from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .select(
        F.col("order_items.quantity"),
        F.col("orders.channel"),
        F.col("orders.customer_id"),
        F.col("orders.order_date"),
    )
)
