from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

qualified = (
    valid.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.city") != "Bangkok"), "left_semi")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

profiled = (
    qualified.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

line_level_moves = (
    profiled.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") < 396.4), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.operator_clean",
        "s.move_type_clean",
        "s.move_count",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    line_level_moves
    .select(
        "id",
        "move_id",
        F.col("move_unit_price").alias("m_move_unit_price"),
        F.col("unit_price").alias("unit_price_v"),
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["m_move_unit_price", "unit_price_v"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
