from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 3) & (~F.col("move_type").isin("ADJUST", "INBOUND")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_dew", "op_chan"), "group_a").when(F.col("operator") == "op_ann", "group_b").otherwise("other").alias("operator_group"),
    )
)

without_matches = (
    cleaned.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

annotated = (
    without_matches
    .withColumn("days_since_moved_at", F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")))
)

result = (
    annotated
    .groupingSets(
        [["sku_id", "warehouse_id", "move_type"], ["sku_id"]],
        "sku_id", "warehouse_id", "move_type",
    )
    .agg(
        F.count(F.when(F.col("move_type") == "INBOUND", 1)).alias("n_move_type_inbound"),
        F.percentile_approx("unit_price", 0.5).alias("median_unit_price"),
    )
    .select(
        "sku_id",
        "warehouse_id",
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "n_move_type_inbound",
        "median_unit_price",
    )
)
