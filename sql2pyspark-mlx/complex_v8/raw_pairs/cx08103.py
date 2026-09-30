from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("qty") <= 372) & (F.col("unit_price") <= 298.9))
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

move_rows = (
    filtered
    .filter((F.month(F.col("moved_at")).isin(1, 2)) & (~F.col("operator").isin("op_bee", "op_eve")))
    .select(
        F.col("move_type").alias("category"),
        "operator",
        F.col("unit_price").alias("metric"),
    )
)

move_rows_2 = (
    df_stock_moves
    .select(
        F.col("operator").alias("category"),
        F.col("move_type").alias("operator"),
        F.col("unit_price").alias("metric"),
    )
)

result = (
    move_rows
    .union(move_rows_2)
    .orderBy("operator")
)
