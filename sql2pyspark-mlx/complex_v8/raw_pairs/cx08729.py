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

outbound_by_sku = (
    df_stock_moves
    .filter(F.col("move_type") == "OUTBOUND")
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("outbound_count"),
        F.round(F.avg("qty"), 2).alias("outbound_avg_qty"),
    )
)

result = (
    inbound_by_sku.alias("a")
    .join(outbound_by_sku.alias("b"), F.col("a.sku_id") == F.col("b.sku_id"), "full")
    .select(
        F.coalesce(F.col("a.sku_id"), F.col("b.sku_id")).alias("sku_id"),
        F.coalesce(F.col("a.inbound_count"), F.lit(0)).alias("inbound_count"),
        F.coalesce(F.col("a.inbound_avg_qty"), F.lit(0)).alias("inbound_avg_qty"),
        F.coalesce(F.col("b.outbound_count"), F.lit(0)).alias("outbound_count"),
        F.col("b.outbound_avg_qty"),
        (F.coalesce(F.col("a.inbound_count"), F.lit(0)) - F.coalesce(F.col("b.outbound_count"), F.lit(0))).alias("inbound_count_minus_outbound_count"),
    )
    .orderBy("sku_id")
)
