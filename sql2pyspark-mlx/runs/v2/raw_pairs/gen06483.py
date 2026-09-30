from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "right")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "right")
    .filter(F.col("order_items.quantity").between(3, 4))
    .groupBy(F.col("orders.status"))
    .agg(
        F.sum(F.col("order_items.unit_price")).alias("total_unit_price"),
        F.sum(F.col("order_items.quantity")).alias("total_quantity"),
    )
)
