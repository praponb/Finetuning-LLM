from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.filter(F.col("delivered_date").between("2024-05-01", "2025-06-28")).alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left_anti")
    .select(F.col("orders.id"), F.col("orders.channel"), F.col("orders.customer_id"))
)
