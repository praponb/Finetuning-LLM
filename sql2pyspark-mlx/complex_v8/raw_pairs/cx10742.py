from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("unit_price") >= 209.4))
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

with_details_warehouse = (
    cleaned_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.max("moved_at").alias("last_moved_at"))
)

with_counts = (
    with_details_warehouse.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.last_moved_at")
)

with_parent = (
    with_counts.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.city") == "Singapore")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.warehouse_opened_on",
        "s.warehouse_wh_code",
        "s.last_moved_at",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.city").alias("warehouse_city"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

with_children_moves = (
    with_parent.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "INBOUND"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.warehouse_wh_code",
        "s.last_moved_at",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

enriched_move = (
    with_children_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.warehouse_wh_code",
        "s.last_moved_at",
        "s.move_id",
        "s.move_type",
        "s.move_qty",
        "s.move_moved_at",
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    enriched_move
    .filter(F.col("move_qty") >= 198)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "warehouse_wh_code",
        "last_moved_at",
        "move_type",
        "move_qty",
        "move_operator",
    )
)
