from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "outer")
    .select(
        F.col("shipments.id"),
        F.col("shipments.delivered_date"),
        F.col("orders.amount"),
        F.col("orders.discount"),
    )
    .orderBy("amount")
)
