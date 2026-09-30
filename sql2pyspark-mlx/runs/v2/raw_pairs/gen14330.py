from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "outer")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .filter(F.col("customers.segment") == "Consumer")
    .select(
        F.col("order_items.product_id"),
        F.col("order_items.unit_price"),
        F.col("orders.customer_id"),
        F.col("orders.status"),
        F.col("customers.is_active"),
        F.col("customers.credit_limit"),
    )
)
