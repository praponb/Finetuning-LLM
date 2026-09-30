from pyspark.sql import functions as F

transfer_by_warehouse = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("transfer_count"),
        F.sum("qty").alias("transfer_total_qty"),
    )
)

outbound_by_warehouse = (
    df_stock_moves
    .filter(F.col("move_type") == "OUTBOUND")
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("outbound_count"),
        F.sum("qty").alias("outbound_total_qty"),
    )
)

result = (
    transfer_by_warehouse.alias("a")
    .join(outbound_by_warehouse.alias("b"), F.col("a.warehouse_id") == F.col("b.warehouse_id"), "full")
    .select(
        F.coalesce(F.col("a.warehouse_id"), F.col("b.warehouse_id")).alias("warehouse_id"),
        F.coalesce(F.col("a.transfer_count"), F.lit(0)).alias("transfer_count"),
        F.col("a.transfer_total_qty"),
        F.coalesce(F.col("b.outbound_count"), F.lit(0)).alias("outbound_count"),
        F.coalesce(F.col("b.outbound_total_qty"), F.lit(0)).alias("outbound_total_qty"),
        (F.coalesce(F.col("a.transfer_count"), F.lit(0)) - F.coalesce(F.col("b.outbound_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("warehouse_id")
)
