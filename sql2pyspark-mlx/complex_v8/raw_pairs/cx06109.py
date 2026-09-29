from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat_ws(": ", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.concat(F.col("operator"), F.lit("#"), F.col("qty").cast("string")).alias("operator_ref"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.min("moved_at").alias("first_moved_at"))
)

profiled = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.first_moved_at")
)

qualified = (
    profiled.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") < 426), "left_anti")
    .filter(F.col("moved_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp"))
)

stats = (
    qualified
    .groupBy("sku_id", "warehouse_id")
    .agg(
        F.round(100 * F.count(F.when(F.col("operator") == "op_bee", 1)) / F.count("*"), 1).alias("pct_operator_op_bee"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.sum(F.when(F.col("operator") == "op_bee", 1).otherwise(0)).alias("n_operator_op_bee"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.sum(F.when(F.col("operator").isin("op_ann", "op_eve"), 1).otherwise(0)).alias("n_operator_in_op_ann_op_eve"),
    )
    .filter(F.col("cnt") >= 2)
)

qualified_2 = (
    stats.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") < 396.4), "left_semi")
)

result = (
    qualified_2
    .select(
        "sku_id",
        "warehouse_id",
        "n_operator_op_bee",
        "n_operator_in_op_ann_op_eve",
        "distinct_move_types",
    )
    .unpivot(
        ids=["sku_id", "warehouse_id"],
        values=["n_operator_op_bee", "n_operator_in_op_ann_op_eve", "distinct_move_types"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
