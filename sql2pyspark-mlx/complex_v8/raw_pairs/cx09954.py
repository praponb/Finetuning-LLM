from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("unit_price") >= 200.7))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.sum("qty").alias("total_move_qty"),
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("move_count"),
    )
)

with_stats = (
    recent_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.total_move_qty",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

line_level = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.hour(F.col("st.moved_at")).between(10, 13)), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    line_level
    .groupBy("sku_id", "move_sku_id")
    .pivot("operator", ["op_bee", "op_chan"])
    .agg(F.avg("qty").alias("avg_v"), F.sum("qty").alias("sum_v"))
)
