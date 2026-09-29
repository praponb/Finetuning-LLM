from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("qty") >= 199)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

screened = (
    scoped_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price").between(112.8, 341.5)), "left_semi")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.sum("qty").alias("total_move_qty"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

with_stats = (
    screened.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.total_move_qty", "st.last_moved_at")
)

summary = (
    with_stats
    .groupBy("warehouse_id")
    .agg(
        F.round(F.avg("total_move_qty"), 2).alias("avg_total_move_qty"),
        F.sum(F.when(F.col("operator") == "op_dew", F.col("days_since_moved_at")).otherwise(0)).alias("days_since_moved_at_operator_op_dew"),
    )
)

result = (
    summary
    .filter(F.col("days_since_moved_at_operator_op_dew") < 2975)
    .select(
        "warehouse_id",
        "avg_total_move_qty",
        "days_since_moved_at_operator_op_dew",
        F.when(F.col("avg_total_move_qty") >= 2100.3, "HIGH").when(F.col("avg_total_move_qty") >= 1050.7, "MEDIUM").otherwise("LOW").alias("avg_total_move_qty_tier"),
        F.round(F.col("avg_total_move_qty") * 0.25, 2).alias("avg_total_move_qty_adj"),
    )
    .orderBy("days_since_moved_at_operator_op_dew", "warehouse_id")
)
