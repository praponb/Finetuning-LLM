from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("unit_price") > 150.7)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

with_children = (
    scoped_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.unit_price",
        "s.least_qty_unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.max("unit_price").alias("max_move_unit_price"),
        F.min("qty").alias("min_move_qty"),
    )
)

profiled = (
    with_children.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.max_move_unit_price",
        "st.min_move_qty",
    )
)

enriched = (
    profiled
    .filter(F.col("move_move_type") == "OUTBOUND")
    .select(
        "*",
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
    )
)

result = (
    enriched
    .select(
        "id",
        "move_id",
        "unit_price",
        "max_move_unit_price",
        "least_qty_unit_price",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["unit_price", "max_move_unit_price", "least_qty_unit_price"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
