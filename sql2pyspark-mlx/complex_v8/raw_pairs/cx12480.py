from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("unit_price").between(58.6, 337.6)) & (F.col("warehouse_id") >= 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

with_counts = (
    cleaned.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

warehouse_id_stats = (
    with_counts
    .groupBy("warehouse_id")
    .agg(F.max("move_count").alias("max_move_count"))
)

above_average = (
    with_counts.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.move_count") == F.col("g.max_move_count"))
    .select(
        "s.*",
        "g.max_move_count",
        F.round(F.col("s.move_count") - F.col("g.max_move_count"), 2).alias("move_count_minus_max"),
    )
)

joined = (
    above_average.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.least_qty_unit_price",
        "s.moved_at_hour",
        "s.move_count",
        "s.move_count_minus_max",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

line_level = (
    joined.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.warehouse_wh_code",
        "s.warehouse_capacity",
        "s.warehouse_opened_on",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    line_level
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        "warehouse_wh_code",
        "warehouse_capacity",
        "warehouse_opened_on",
        "move_id",
        "move_qty",
        "move_sku_id",
        "move_moved_at",
        F.when(F.col("move_unit_price") >= 363.6, "HIGH").when(F.col("move_unit_price") >= 124.1, "MEDIUM").otherwise("LOW").alias("move_unit_price_tier"),
        (F.col("moved_at") + F.expr("INTERVAL 2 HOURS")).alias("moved_at_plus_2h"),
    )
    .orderBy(F.col("moved_at").asc(), "id", "move_id")
    .limit(5)
)
