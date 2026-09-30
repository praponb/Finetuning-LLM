from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.customer_id").isNull() & F.col("order_items.product_id").isin([8, 12, 25]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("order_items.quantity"),
        F.col("order_items.product_id"),
        F.col("orders.status"),
        F.col("orders.customer_id"),
        F.col("orders.store_id"),
    )
)
