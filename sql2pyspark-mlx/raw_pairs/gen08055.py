from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("sbo.in_transit_count"),
        F.col("sbo.first_shipped_date"),
    )
    .orderBy("first_shipped_date")
)
