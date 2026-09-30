from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter((F.col("move_type") == "TRANSFER") & (F.col("unit_price") >= 61.2))
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
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
    )
)

enriched = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.opened_on") >= F.lit("2024-04-13").cast("date"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.move_type_len",
        "s.moved_at_date",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") < 292.3), "left_semi")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("qty").alias("max_move_qty"),
        F.sum("qty").alias("total_move_qty"),
    )
)

profiled = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.max_move_qty", "st.total_move_qty")
)

with_parent = (
    profiled.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at_date",
        "s.warehouse_capacity",
        "s.warehouse_opened_on",
        "sk.sku_code",
    )
)

enriched_2 = (
    with_parent
    .filter(F.col("warehouse_opened_on") >= F.lit("2024-04-13").cast("date"))
    .select(
        "*",
        F.date_add(F.col("warehouse_opened_on"), 30).alias("warehouse_opened_on_plus_30d"),
        F.when(F.col("warehouse_capacity") >= 11506, "HIGH").when(F.col("warehouse_capacity") >= 11005, "MEDIUM").otherwise("LOW").alias("warehouse_capacity_tier"),
    )
)

result = (
    enriched_2
    .groupingSets(
        [["warehouse_id", "move_type", "sku_id"], ["warehouse_id"], ["move_type"], []],
        "warehouse_id", "move_type", "sku_id",
    )
    .agg(F.count("*").alias("row_count"))
    .select(
        "warehouse_id",
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "sku_id",
        "row_count",
    )
)
