from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.month(F.col("moved_at")).isin(1, 3))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.round(F.avg("qty"), 2).alias("avg_move_qty"),
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
    )
)

with_counts = (
    valid_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.avg_move_qty",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

enriched_warehouse = (
    with_counts.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator",
        "s.unit_price",
        "s.moved_at_dayofweek",
        "s.avg_move_qty",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.capacity").alias("warehouse_capacity"),
    )
)

line_level_moves = (
    enriched_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.avg_move_qty",
        "s.warehouse_city",
        "s.warehouse_capacity",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
        "st.move_type",
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

result = (
    line_level_moves
    .cube("move_type", "warehouse_id")
    .agg(
        F.sum(F.when(F.col("warehouse_city").isin("Kuala Lumpur", "Singapore"), 1).otherwise(0)).alias("n_warehouse_city_in_kuala_lump"),
        F.array_sort(F.collect_set("warehouse_city")).alias("warehouse_city_list"),
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
