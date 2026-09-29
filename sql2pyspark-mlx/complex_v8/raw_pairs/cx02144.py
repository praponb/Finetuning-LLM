from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter((F.col("unit_price").between(130.6, 363.6)) & (F.col("qty").between(116, 357)))
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

enriched = (
    scoped_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "*",
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
        F.count("*").alias("move_count"),
        F.max("qty").alias("max_move_qty"),
    )
)

summarized_rows = (
    enriched.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.avg_move_unit_price",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.max_move_qty",
    )
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty").between(89, 393)), "left_semi")
    .filter(F.col("qty_minus_unit_price").between(200.8, 700.3))
)

result = (
    with_activity
    .groupBy("move_type")
    .pivot("operator", ["op_ann", "op_chan", "op_bee"])
    .agg(F.sum("qty_minus_unit_price"))
)
