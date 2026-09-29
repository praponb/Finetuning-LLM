from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter((F.col("unit_price") >= 125.9) & (F.col("moved_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
    )
)

rollup_base = (
    recent_moves
    .groupBy("operator", "warehouse_id", "sku_id")
    .agg(
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.sum(F.when(F.col("operator") != "op_ann", F.col("qty")).otherwise(0)).alias("qty_operator_op_ann"),
        F.round(F.sum("unit_price") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("unit_price_to_qty_ratio"),
        F.sum(F.when(F.col("operator") == "op_dew", 1).otherwise(0)).alias("n_operator_op_dew"),
    )
)

combined_details = (
    rollup_base.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.category") == "Toys"), "inner")
    .select(
        "s.operator",
        "s.warehouse_id",
        "s.sku_id",
        "s.operator_list",
        "s.qty_operator_op_ann",
        "s.unit_price_to_qty_ratio",
        "s.n_operator_op_dew",
        F.col("sk.is_active").alias("sku_is_active"),
        "sk.sku_code",
    )
)

result = (
    combined_details
    .select(
        "operator",
        "warehouse_id",
        "operator_list",
        "unit_price_to_qty_ratio",
    )
    .orderBy(
        F.col("unit_price_to_qty_ratio").desc(),
        F.col("operator").asc_nulls_last(),
    )
)
