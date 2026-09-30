from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.month("shipped_date").isin([2, 4, 12]))
    .groupBy("order_id")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "left")
    .select(F.col("o.store_id"), F.col("sbo.cost_range"))
    .orderBy(F.col("cost_range").desc())
)
