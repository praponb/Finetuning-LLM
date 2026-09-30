from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

exploded_detail_moves = (
    filtered_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

enriched = (
    exploded_detail_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "left")
    .select(
        "s.*",
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.move_type").alias("move_move_type_2"),
        F.col("st.qty").alias("move_qty"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "ADJUST") & (F.col("st.moved_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("qty") >= 198)
)

with_activity = (
    filtered
    .crossJoin(filtered.agg(F.min("move_qty").alias("overall_min_move_qty")))
    .filter(F.col("move_qty") > F.col("overall_min_move_qty"))
    .drop("overall_min_move_qty")
)

result = (
    with_activity
    .filter(F.col("move_moved_at") >= F.lit("2025-01-25 07:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "move_id",
        "move_operator",
        "move_move_type",
        "move_moved_at",
        "move_move_type_2",
        "move_qty",
        F.when(F.col("move_type").isin("INBOUND", "TRANSFER"), "group_a").when(F.col("move_type") == "ADJUST", "group_b").otherwise("other").alias("move_type_group"),
        F.date_format(F.col("move_moved_at"), "yyyy-MM").alias("move_moved_at_label"),
    )
    .orderBy(F.col("move_type_group").desc(), "sku_id")
)
