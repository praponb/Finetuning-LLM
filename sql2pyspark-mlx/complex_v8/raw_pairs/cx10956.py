from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

detail_rows = (
    base_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type").isNotNull()), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        F.col("st.id").alias("move_id"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

screened = (
    detail_rows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (~F.col("st.operator").isin("op_dew", "op_eve")), "left_semi")
    .filter(F.col("moved_at") < F.lit("2025-03-02 13:00:00").cast("timestamp"))
)

stats = (
    screened
    .filter(F.col("move_sku_id").isNotNull())
    .groupBy("move_move_type", "warehouse_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.round(F.avg("move_unit_price"), 2).alias("avg_move_unit_price"),
        F.round(F.sum("move_unit_price"), 2).alias("total_move_unit_price"),
        F.count("*").alias("row_count"),
    )
)

screened_2 = (
    stats.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.month(F.col("st.moved_at")).isin(2, 3)), "left_semi")
    .filter(F.col("total_move_unit_price") >= 1050.7)
)

enriched_warehouse = (
    screened_2.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.move_move_type",
        "s.warehouse_id",
        "s.first_moved_at",
        "s.avg_move_unit_price",
        "s.row_count",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

result = (
    enriched_warehouse
    .select(
        "move_move_type",
        "warehouse_id",
        "first_moved_at",
        "avg_move_unit_price",
        "row_count",
        "warehouse_opened_on",
        F.when(F.col("move_move_type").isin("TRANSFER", "ADJUST"), "group_a").when(F.col("move_move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_move_type_group"),
    )
    .orderBy(
        F.col("warehouse_opened_on").desc_nulls_first(),
        F.col("move_move_type_group").desc(),
    )
)
