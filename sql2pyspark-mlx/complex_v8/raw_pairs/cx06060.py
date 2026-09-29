from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter(~F.col("move_type").isin("TRANSFER", "ADJUST"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator") == "op_dew", 1)).alias("move_operator_count"),
    )
)

summarized_rows = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

screened = (
    summarized_rows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator") != "op_dew"), "left_anti")
)

result = (
    screened
    .groupBy("moved_at", "move_type")
    .pivot("operator", ["op_chan", "op_dew"])
    .agg(F.min("move_operator_count"))
)
