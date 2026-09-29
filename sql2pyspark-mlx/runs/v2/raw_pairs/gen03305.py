from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(F.col("o.store_id"), F.col("o.discount"), F.col("t.cnt"))
)
