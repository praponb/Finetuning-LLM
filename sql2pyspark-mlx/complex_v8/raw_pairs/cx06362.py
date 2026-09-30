from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-02-18 14:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
        F.abs(F.col("qty") - 139).alias("qty_distance"),
    )
)

without_matches = (
    valid.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.move_type") == "ADJUST") & (F.col("st.qty") < 387), "left_anti")
)

qualified = (
    without_matches.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (~F.col("st.move_type").isin("ADJUST", "INBOUND")), "left_anti")
)

result = (
    qualified
    .groupingSets(
        [["move_type", "warehouse_id", "sku_id"], ["move_type"], []],
        "move_type", "warehouse_id", "sku_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("operator") == "op_ann", F.col("qty")).otherwise(0)).alias("qty_operator_op_ann"),
        F.count("*").alias("cnt"),
        F.round(F.sum("unit_price"), 2).alias("total_unit_price"),
    )
    .select(
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "warehouse_id",
        "sku_id",
        "gid",
        "qty_operator_op_ann",
        "cnt",
        "total_unit_price",
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
