from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.order_date") >= "2025-06-01")
    .groupBy(F.col("o.id"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.sum(F.col("s.cost")).alias("total_cost"),
    )
    .filter(F.col("total_cost") >= 410)
)
