from pyspark.sql import functions as F

outbound_by_warehouse = (
    df_stock_moves
    .filter(F.col("move_type") == "OUTBOUND")
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("outbound_count"),
        F.round(F.avg("qty"), 2).alias("outbound_avg_qty"),
    )
)

inbound_by_warehouse = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("inbound_count"),
        F.sum("qty").alias("inbound_total_qty"),
    )
)

result = (
    outbound_by_warehouse.alias("a")
    .join(inbound_by_warehouse.alias("b"), F.col("a.warehouse_id") == F.col("b.warehouse_id"), "full")
    .select(
        F.coalesce(F.col("a.warehouse_id"), F.col("b.warehouse_id")).alias("warehouse_id"),
        F.coalesce(F.col("a.outbound_count"), F.lit(0)).alias("outbound_count"),
        F.col("a.outbound_avg_qty"),
        F.coalesce(F.col("b.inbound_count"), F.lit(0)).alias("inbound_count"),
        F.col("b.inbound_total_qty"),
        (F.coalesce(F.col("a.outbound_count"), F.lit(0)) - F.coalesce(F.col("b.inbound_count"), F.lit(0))).alias("outbound_count_minus_inbound_count"),
    )
    .orderBy("warehouse_id")
)
