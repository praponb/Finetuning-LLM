from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("move_type").isin("TRANSFER", "OUTBOUND")) | (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
    )
)

filtered = (
    valid_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") > 154) & (F.col("st.sku_id").isNotNull()), "left_semi")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type").isNotNull(), 1)).alias("move_move_type_count"),
        F.max("qty").alias("max_move_qty"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        "st.max_move_qty",
    )
)

result = (
    with_counts
    .select(
        "id",
        "move_type",
        "moved_at",
        "unit_price",
        "moved_at_plus_30m",
        "move_move_type_count",
        "max_move_qty",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
    .orderBy("max_move_qty")
)
