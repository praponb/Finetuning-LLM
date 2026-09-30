from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "right")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.length(F.col("customers.name")) > 10)
    .select(
        F.col("shipments.status"),
        F.col("orders.status").alias("order_status"),
        F.col("orders.store_id"),
        F.col("customers.email"),
    )
)
