from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

qualified = (
    recent_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") > 95.8) & (F.col("st.warehouse_id") <= 5), "left_anti")
)

enriched = (
    qualified.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_len",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

result = (
    enriched
    .filter(F.month(F.col("moved_at")).isin(1, 3))
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "move_type_len",
        "warehouse_capacity",
        "warehouse_wh_code",
        "warehouse_opened_on",
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
        F.date_format(F.col("warehouse_opened_on"), "yyyy-MM-dd").alias("warehouse_opened_on_label"),
    )
    .orderBy(
        F.col("warehouse_opened_on_label").desc(),
        F.col("moved_at_label").desc(),
    )
)
