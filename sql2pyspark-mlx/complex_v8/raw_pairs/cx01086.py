from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter(F.col("qty") <= 387)
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

stats = (
    base
    .filter(F.col("unit_price") > 164.1)
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.round(100 * F.count(F.when(F.col("operator") == "op_bee", 1)) / F.count("*"), 1).alias("pct_operator_op_bee"),
        F.round(F.sum("unit_price") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("unit_price_to_qty_ratio"),
        F.min("moved_at").alias("first_moved_at"),
        F.count(F.when(F.col("move_type") == "ADJUST", 1)).alias("n_move_type_adjust"),
    )
)

result = (
    stats
    .select(
        "warehouse_id",
        F.col("pct_operator_op_bee").alias("pct_operator_op_bee_v"),
        F.col("unit_price_to_qty_ratio").alias("m_unit_price_to_qty_ratio"),
    )
    .unpivot(
        ids=["warehouse_id"],
        values=["pct_operator_op_bee_v", "m_unit_price_to_qty_ratio"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
