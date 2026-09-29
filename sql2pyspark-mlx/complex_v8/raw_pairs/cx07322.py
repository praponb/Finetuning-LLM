from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date")) & (F.col("operator") == "op_ann"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_eve", "op_ann"), "group_a").when(F.col("operator") == "op_bee", "group_b").otherwise("other").alias("operator_group"),
        (F.col("moved_at") + F.expr("INTERVAL 2 HOURS")).alias("moved_at_plus_2h"),
    )
)

labeled = (
    recent
    .filter(F.col("unit_price") < 387.1)
    .select(
        "*",
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
)

with_children_moves = (
    labeled.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.move_type") == "ADJUST"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.operator_group",
        "s.unit_price_adj",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

labeled_2 = (
    with_children_moves
    .select(
        "*",
        F.round(F.col("unit_price_adj") * 1.5, 2).alias("unit_price_adj_adj"),
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.when(F.col("move_operator").isin("op_dew", "op_ann"), "group_a").when(F.col("move_operator") == "op_bee", "group_b").otherwise("other").alias("move_operator_group"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.sum("unit_price").alias("total_move_unit_price"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

summarized_rows = (
    labeled_2.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.total_move_unit_price", "st.last_moved_at")
)

qualified = (
    summarized_rows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") < 323.9) & (F.col("st.warehouse_id") >= 4), "left_anti")
)

result = (
    qualified
    .filter(F.col("last_moved_at") < F.lit("2025-02-09 09:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price_adj",
        "move_operator",
        "move_move_type",
        "move_type_or_unknown",
        "total_move_unit_price",
        F.when(F.col("move_type_or_unknown").isin("TRANSFER", "INBOUND"), "group_a").when(F.col("move_type_or_unknown") == "UNKNOWN", "group_b").otherwise("other").alias("move_type_or_unknown_group"),
        F.concat(F.col("move_operator_group"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_operator_group_ref"),
    )
)
