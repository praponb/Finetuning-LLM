from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")) & (F.col("move_type").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.when(F.col("qty") >= 394, "large").when(F.col("qty") >= 99, "medium").otherwise("small").alias("qty_tier"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
    )
)

without_matches = (
    recent_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.capacity") >= 2614), "left_anti")
)

result = (
    without_matches
    .groupBy("warehouse_id", "sku_id")
    .pivot("move_type", ["OUTBOUND", "ADJUST"])
    .agg(F.max("qty").alias("max_v"), F.count("qty").alias("n"))
)
