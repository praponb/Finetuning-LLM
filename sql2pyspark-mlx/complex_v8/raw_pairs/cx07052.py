from pyspark.sql import functions as F

inbound_monthly = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .groupBy(F.date_trunc("month", "moved_at").alias("moved_at_month"))
    .agg(
        F.count("*").alias("inbound_count"),
        F.sum("qty").alias("inbound_total_qty"),
    )
)

transfer_monthly = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .groupBy(F.date_trunc("month", "moved_at").alias("moved_at_month"))
    .agg(
        F.count("*").alias("transfer_count"),
        F.round(F.avg("qty"), 2).alias("transfer_avg_qty"),
    )
)

result = (
    inbound_monthly.alias("a")
    .join(transfer_monthly.alias("b"), F.col("a.moved_at_month") == F.col("b.moved_at_month"), "full")
    .select(
        F.coalesce(F.col("a.moved_at_month"), F.col("b.moved_at_month")).alias("moved_at_month"),
        F.coalesce(F.col("a.inbound_count"), F.lit(0)).alias("inbound_count"),
        F.coalesce(F.col("a.inbound_total_qty"), F.lit(0)).alias("inbound_total_qty"),
        F.coalesce(F.col("b.transfer_count"), F.lit(0)).alias("transfer_count"),
        F.coalesce(F.col("b.transfer_avg_qty"), F.lit(0)).alias("transfer_avg_qty"),
        (F.coalesce(F.col("a.inbound_count"), F.lit(0)) - F.coalesce(F.col("b.transfer_count"), F.lit(0))).alias("inbound_count_minus_transfer_count"),
    )
    .orderBy("moved_at_month")
)
