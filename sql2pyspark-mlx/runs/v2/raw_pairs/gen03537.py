from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .select(
        F.col("shipments.id"),
        F.col("shipments.order_id"),
        F.col("shipments.delivered_date"),
        F.col("shipments.cost"),
        F.col("orders.amount"),
    )
    .orderBy(F.col("order_id").desc(), "delivered_date")
)
