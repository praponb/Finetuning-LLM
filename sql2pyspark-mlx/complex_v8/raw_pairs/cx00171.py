from pyspark.sql import functions as F

transfer_by_warehouse = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("transfer_count"),
        F.round(F.avg("qty"), 2).alias("transfer_avg_qty"),
    )
)

inbound_by_warehouse = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("inbound_count"),
        F.round(F.avg("qty"), 2).alias("inbound_avg_qty"),
    )
)

result = (
    transfer_by_warehouse.alias("a")
    .join(inbound_by_warehouse.alias("b"), F.col("a.warehouse_id") == F.col("b.warehouse_id"), "full")
    .select(
        F.coalesce(F.col("a.warehouse_id"), F.col("b.warehouse_id")).alias("warehouse_id"),
        F.coalesce(F.col("a.transfer_count"), F.lit(0)).alias("transfer_count"),
        F.coalesce(F.col("a.transfer_avg_qty"), F.lit(0)).alias("transfer_avg_qty"),
        F.coalesce(F.col("b.inbound_count"), F.lit(0)).alias("inbound_count"),
        F.coalesce(F.col("b.inbound_avg_qty"), F.lit(0)).alias("inbound_avg_qty"),
        (F.coalesce(F.col("a.transfer_count"), F.lit(0)) - F.coalesce(F.col("b.inbound_count"), F.lit(0))).alias("transfer_count_minus_inbound_count"),
    )
    .orderBy("warehouse_id")
)
