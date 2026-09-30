from pyspark.sql import functions as F

transfer_by_sku = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("transfer_count"),
        F.sum("qty").alias("transfer_total_qty"),
    )
)

outbound_by_sku = (
    df_stock_moves
    .filter(F.col("move_type") == "OUTBOUND")
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("outbound_count"),
        F.sum("qty").alias("outbound_total_qty"),
    )
)

result = (
    transfer_by_sku.alias("a")
    .join(outbound_by_sku.alias("b"), F.col("a.sku_id") == F.col("b.sku_id"), "full")
    .select(
        F.coalesce(F.col("a.sku_id"), F.col("b.sku_id")).alias("sku_id"),
        F.coalesce(F.col("a.transfer_count"), F.lit(0)).alias("transfer_count"),
        F.col("a.transfer_total_qty"),
        F.coalesce(F.col("b.outbound_count"), F.lit(0)).alias("outbound_count"),
        F.col("b.outbound_total_qty"),
        (F.coalesce(F.col("a.transfer_count"), F.lit(0)) - F.coalesce(F.col("b.outbound_count"), F.lit(0))).alias("transfer_count_minus_outbound_count"),
    )
    .orderBy("sku_id")
)
