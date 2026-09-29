from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("move_type").isin("ADJUST", "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 268).alias("qty_distance"),
        F.concat_ws("|", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

warehouse_id_stats = (
    filtered_moves
    .groupBy("warehouse_id")
    .agg(F.avg("qty_distance").alias("avg_qty_distance"))
)

vs_group = (
    filtered_moves.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.qty_distance") > F.col("g.avg_qty_distance"))
    .select(
        "s.*",
        "g.avg_qty_distance",
        F.round(F.col("s.qty_distance") - F.col("g.avg_qty_distance"), 2).alias("qty_distance_minus_avg"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.sum("unit_price").alias("total_move_unit_price"),
        F.count(F.when(F.col("operator") == "op_dew", 1)).alias("move_operator_count"),
    )
)

with_counts = (
    vs_group.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        "st.total_move_unit_price",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "sku_id",
        "operator",
        "unit_price",
        "qty_distance",
        "operator_move_type_label",
        "avg_qty_distance",
        "qty_distance_minus_avg",
        "total_move_unit_price",
        "move_operator_count",
    )
    .orderBy(F.col("qty_distance").desc())
)
