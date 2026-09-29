from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (~F.col("operator").isin("op_ann", "op_bee")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.sum("unit_price").alias("total_move_unit_price"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

with_counts = (
    prepared_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.total_move_unit_price", "st.first_moved_at")
)

labeled = (
    with_counts
    .filter(F.col("warehouse_id") >= 4)
    .withColumn("days_since_first_moved_at", F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_moved_at")))
)

result = (
    labeled
    .groupBy("warehouse_id", "sku_id")
    .pivot("operator_or_unknown", ["op_dew", "op_bee"])
    .agg(F.max("unit_price").alias("max_v"), F.sum("unit_price").alias("sum_v"))
)
