from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("move_type").isin("OUTBOUND", "INBOUND"), 1)).alias("move_move_type_count"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

summarized_rows = (
    scoped_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        "st.last_moved_at",
    )
)

with_details_warehouse = (
    summarized_rows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price",
        "s.move_move_type_count",
        "s.last_moved_at",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

grouped = (
    with_details_warehouse
    .groupBy("warehouse_id", "warehouse_city")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.array_sort(F.collect_set("warehouse_city")).alias("warehouse_city_list"),
    )
)

without_matches = (
    grouped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id") >= 8), "left_anti")
)

result = (
    without_matches
    .select("warehouse_id", "min_unit_price")
    .orderBy(
        F.col("min_unit_price").desc(),
        F.col("warehouse_id").desc_nulls_first(),
    )
)
