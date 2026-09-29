from pyspark.sql import functions as F

inbound_by_sku = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("inbound_count"),
        F.round(F.avg("qty"), 2).alias("inbound_avg_qty"),
    )
)

transfer_by_sku = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("transfer_count"),
        F.sum("qty").alias("transfer_total_qty"),
    )
)

result = (
    inbound_by_sku.alias("a")
    .join(transfer_by_sku.alias("b"), F.col("a.sku_id") == F.col("b.sku_id"), "full")
    .select(
        F.coalesce(F.col("a.sku_id"), F.col("b.sku_id")).alias("sku_id"),
        F.coalesce(F.col("a.inbound_count"), F.lit(0)).alias("inbound_count"),
        F.coalesce(F.col("a.inbound_avg_qty"), F.lit(0)).alias("inbound_avg_qty"),
        F.coalesce(F.col("b.transfer_count"), F.lit(0)).alias("transfer_count"),
        F.col("b.transfer_total_qty"),
        (F.coalesce(F.col("a.inbound_count"), F.lit(0)) - F.coalesce(F.col("b.transfer_count"), F.lit(0))).alias("net_count"),
    )
    .orderBy("sku_id")
)
