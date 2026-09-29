from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("order_id") == 38)
    .groupBy("order_id")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.cost_range") >= 66)
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("t.cost_range"),
        F.col("t.lost_count"),
    )
)
