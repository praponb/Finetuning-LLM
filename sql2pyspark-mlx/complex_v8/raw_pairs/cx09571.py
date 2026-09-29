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
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count(F.when(F.col("move_type").isin("TRANSFER", "INBOUND", "OUTBOUND"), 1)).alias("move_move_type_count"),
    )
)

with_counts = (
    scoped_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

exploded_detail = (
    with_counts.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id") <= 5), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.last_moved_at",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

result = (
    exploded_detail
    .rollup("move_type", "warehouse_id", "operator")
    .agg(
        F.count(F.when(F.col("move_type").isin("INBOUND", "TRANSFER"), 1)).alias("n_move_type_in_inbound_transfe"),
    )
)
