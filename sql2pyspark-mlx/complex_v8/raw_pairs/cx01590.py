from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 4) & (F.col("unit_price") <= 330.0))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

screened = (
    recent_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

result = (
    screened
    .cube("warehouse_id", "move_type")
    .agg(
        F.round(F.sum("unit_price") / F.nullif(F.sum("qty"), F.lit(0)), 3).alias("unit_price_to_qty_ratio"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.sum(F.when(F.col("operator") == "op_chan", F.col("qty_filled"))).alias("qty_filled_operator_op_chan"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
