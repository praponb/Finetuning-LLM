from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter(F.hour(F.col("moved_at")).between(10, 14))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
    )
)

move_type_stats = (
    recent_moves
    .groupBy("move_type")
    .agg(F.max("qty").alias("max_qty"))
)

outliers = (
    recent_moves.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.qty") == F.col("g.max_qty"))
    .select(
        "s.*",
        "g.max_qty",
        F.round(F.col("s.qty") - F.col("g.max_qty"), 2).alias("qty_minus_max"),
    )
)

line_level = (
    outliers.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.max_qty",
        "s.qty_minus_max",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

result = (
    line_level
    .select(
        "id",
        "move_id",
        "qty_minus_max",
        "max_qty",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["qty_minus_max", "max_qty"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
