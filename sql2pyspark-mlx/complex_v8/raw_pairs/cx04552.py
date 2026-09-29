from pyspark.sql import functions as F

inbound_by_warehouse = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("inbound_count"),
        F.round(F.avg("qty"), 2).alias("inbound_avg_qty"),
    )
)

transfer_by_warehouse = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("transfer_count"),
        F.sum("qty").alias("transfer_total_qty"),
    )
)

result = (
    inbound_by_warehouse.alias("a")
    .join(transfer_by_warehouse.alias("b"), F.col("a.warehouse_id") == F.col("b.warehouse_id"), "full")
    .select(
        F.coalesce(F.col("a.warehouse_id"), F.col("b.warehouse_id")).alias("warehouse_id"),
        F.coalesce(F.col("a.inbound_count"), F.lit(0)).alias("inbound_count"),
        F.col("a.inbound_avg_qty"),
        F.coalesce(F.col("b.transfer_count"), F.lit(0)).alias("transfer_count"),
        F.col("b.transfer_total_qty"),
        (F.coalesce(F.col("a.inbound_count"), F.lit(0)) - F.coalesce(F.col("b.transfer_count"), F.lit(0))).alias("inbound_count_minus_transfer_count"),
    )
    .orderBy("warehouse_id")
)
