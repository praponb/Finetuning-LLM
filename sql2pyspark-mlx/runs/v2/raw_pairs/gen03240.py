from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .select(
        F.col("orders.discount"),
        F.col("orders.customer_id"),
        F.col("shipments.order_id"),
        F.col("shipments.cost"),
    )
)
