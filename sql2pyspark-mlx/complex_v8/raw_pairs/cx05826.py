from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter(F.col("unit_price") < 292.3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
    )
)

line_level = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator_or_unknown",
        "s.move_type_ref",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

result = (
    line_level
    .groupingSets(
        [["move_operator", "move_warehouse_id"], ["move_operator"], ["move_warehouse_id"]],
        "move_operator", "move_warehouse_id",
    )
    .agg(
        F.count(F.when(F.col("move_operator").isin("op_ann", "op_bee"), 1)).alias("n_move_operator_in_op_ann_op_b"),
    )
    .orderBy(
        F.col("move_operator").asc_nulls_last(),
        F.col("move_warehouse_id").asc_nulls_last(),
    )
)
