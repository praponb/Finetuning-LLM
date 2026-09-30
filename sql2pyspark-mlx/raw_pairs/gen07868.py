from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(F.count("cost").alias("cost_count"))
)

result = (
    df_orders.alias("orders")
    .join(shipments_by_order.alias("shipments_by_order"), F.col("orders.id") == F.col("shipments_by_order.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("orders.customer_id"),
        F.col("shipments_by_order.cost_count"),
    )
    .orderBy(F.col("id").desc())
)
