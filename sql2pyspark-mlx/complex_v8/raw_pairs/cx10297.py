from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter(F.col("unit_price") >= 209.4)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.when(F.col("move_type").isin("TRANSFER", "INBOUND"), "group_a").when(F.col("move_type") == "OUTBOUND", "group_b").otherwise("other").alias("move_type_group"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.concat_ws(" / ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

line_level_moves = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "OUTBOUND"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.move_type_group",
        "s.move_type_operator_label",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.qty").alias("move_qty"),
    )
)

derived = (
    line_level_moves
    .select(
        "*",
        F.substring(F.trim(F.col("move_operator")), 1, 2).alias("move_operator_prefix"),
    )
)

without_matches = (
    derived
    .crossJoin(derived.agg(F.max("move_qty").alias("overall_max_move_qty")))
    .filter(F.col("move_qty") < F.col("overall_max_move_qty"))
    .drop("overall_max_move_qty")
)

result = (
    without_matches
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "move_type_group",
        "move_type_operator_label",
        "move_id",
        "move_move_type",
        "move_moved_at",
        "move_operator",
        "move_operator_prefix",
        (F.floor(F.col("move_qty") / 100) * 100).alias("move_qty_bucket"),
    )
    .orderBy(
        F.col("warehouse_id").desc(),
        "move_moved_at",
        "id",
        "move_id",
    )
    .limit(5)
)
