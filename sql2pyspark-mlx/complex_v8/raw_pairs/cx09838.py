from pyspark.sql import functions as F

outbound_by_sku = (
    df_stock_moves
    .filter(F.col("move_type") == "OUTBOUND")
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("outbound_count"),
        F.sum("qty").alias("outbound_total_qty"),
    )
)

inbound_by_sku = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("inbound_count"),
        F.sum("qty").alias("inbound_total_qty"),
    )
)

result = (
    outbound_by_sku.alias("a")
    .join(inbound_by_sku.alias("b"), F.col("a.sku_id") == F.col("b.sku_id"), "full")
    .select(
        F.coalesce(F.col("a.sku_id"), F.col("b.sku_id")).alias("sku_id"),
        F.coalesce(F.col("a.outbound_count"), F.lit(0)).alias("outbound_count"),
        F.col("a.outbound_total_qty"),
        F.coalesce(F.col("b.inbound_count"), F.lit(0)).alias("inbound_count"),
        F.col("b.inbound_total_qty"),
        (F.coalesce(F.col("a.outbound_count"), F.lit(0)) - F.coalesce(F.col("b.inbound_count"), F.lit(0))).alias("outbound_count_minus_inbound_count"),
    )
    .orderBy("sku_id")
)
