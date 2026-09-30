from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter(F.col("qty") >= 191)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 271).alias("qty_distance"),
        F.abs(F.col("unit_price") - 277.6).alias("unit_price_distance"),
    )
)

move_type_stats = (
    scoped
    .groupBy("move_type")
    .agg(F.avg("qty_distance").alias("avg_qty_distance"))
)

outliers = (
    scoped.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.qty_distance") > F.col("g.avg_qty_distance"))
    .select(
        "s.*",
        "g.avg_qty_distance",
        F.round(F.col("s.qty_distance") - F.col("g.avg_qty_distance"), 2).alias("qty_distance_minus_avg"),
    )
)

with_children = (
    outliers.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price_distance",
        "s.avg_qty_distance",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

result = (
    with_children
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "avg_qty_distance",
        "move_unit_price",
        "move_warehouse_id",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.when(F.col("avg_qty_distance") >= 375.0, "HIGH").when(F.col("avg_qty_distance") >= 200.0, "MEDIUM").otherwise("LOW").alias("avg_qty_distance_tier"),
    )
)
