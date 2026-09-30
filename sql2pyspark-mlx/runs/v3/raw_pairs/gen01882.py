from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.countDistinct("cost").alias("unique_statuss"),
    )
)

result = (
    df_orders.alias("orders")
    .join(shipments_by_order.alias("t"), F.col("orders.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.unique_statuss") >= 5)
    .select(
        F.col("orders.id"),
        F.col("orders.channel"),
        F.col("orders.customer_id"),
        F.col("t.min_cost"),
        F.col("t.unique_statuss"),
    )
)
