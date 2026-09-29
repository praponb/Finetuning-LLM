from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left")
    .filter(F.col("orders.customer_id").isNotNull())
    .select(
        F.col("orders.status"),
        F.col("orders.channel"),
        F.col("orders.amount"),
        F.col("shipments.cost"),
        F.col("shipments.delivered_date"),
    )
)
