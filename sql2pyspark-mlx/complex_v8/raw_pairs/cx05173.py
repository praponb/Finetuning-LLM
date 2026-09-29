from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter((F.col("operator") == "op_bee") & (F.col("unit_price") <= 387.1))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
)

line_level_moves = (
    filtered_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "INBOUND"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.moved_at_hour",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
    )
)

with_children_moves = (
    line_level_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator") == "op_eve"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.move_id",
        "s.move_operator",
        "s.move_moved_at",
        "s.move_qty",
        F.col("st.id").alias("move_id_2"),
        F.col("st.operator").alias("move_operator_2"),
        F.col("st.moved_at").alias("move_moved_at_2"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty_2"),
    )
)

with_activity = (
    with_children_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator") != "op_bee") & (F.col("st.qty") > 148), "left_anti")
)

per_group = (
    with_activity
    .groupBy(F.date_trunc("month", "move_moved_at").alias("move_moved_at_month"))
    .agg(
        F.sum("move_unit_price").alias("total_move_unit_price"),
        F.count(F.when(F.col("operator") == "op_dew", 1)).alias("n_operator_op_dew"),
    )
)

enriched = (
    per_group
    .select(
        "move_moved_at_month",
        "total_move_unit_price",
        "n_operator_op_dew",
        F.round(F.col("n_operator_op_dew") / F.nullif(F.col("total_move_unit_price"), F.lit(0)), 3).alias("n_operator_op_dew_per_total_move_unit_price"),
        F.coalesce(F.col("n_operator_op_dew"), F.lit(0)).alias("n_operator_op_dew_filled"),
        F.date_trunc("day", F.col("move_moved_at_month")).alias("move_moved_at_month_day"),
    )
)

result = (
    enriched
    .filter(F.col("n_operator_op_dew") >= 2)
    .select(
        "move_moved_at_month",
        "total_move_unit_price",
        "n_operator_op_dew",
        "n_operator_op_dew_per_total_move_unit_price",
        "move_moved_at_month_day",
        F.date_trunc("week", F.col("move_moved_at_month")).alias("move_moved_at_month_week"),
    )
    .orderBy("total_move_unit_price")
)
