from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
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
        (F.col("moved_at") + F.expr("INTERVAL 15 MINUTES")).alias("moved_at_plus_15m"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

exploded_detail_moves = (
    scoped_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.move_type") == "TRANSFER"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.qty_minus_unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

result = (
    exploded_detail_moves
    .groupBy("move_move_type", "move_moved_at")
    .pivot("operator", ["op_dew", "op_ann"])
    .agg(F.max("unit_price"))
    .orderBy("move_move_type", "move_moved_at")
)
