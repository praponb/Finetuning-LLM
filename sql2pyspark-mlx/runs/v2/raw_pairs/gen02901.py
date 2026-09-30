from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .groupBy("order_id")
    .agg(F.avg("cost").alias("avg_cost"))
)

result = (
    df_orders.alias("orders")
    .join(order_summary.alias("t"), F.col("orders.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.avg_cost") >= 28)
    .select(F.col("orders.status"), F.col("orders.customer_id"), F.col("t.avg_cost"))
    .orderBy("avg_cost")
)
