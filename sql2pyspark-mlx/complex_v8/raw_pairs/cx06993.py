from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) & (F.col("move_type") != "TRANSFER"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 138).alias("qty_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

without_matches = (
    recent_moves
    .crossJoin(recent_moves.agg(F.min("qty_distance").alias("overall_min_qty_distance")))
    .filter(F.col("qty_distance") > F.col("overall_min_qty_distance"))
    .drop("overall_min_qty_distance")
)

result = (
    without_matches
    .cube("move_type", "operator", "sku_id")
    .agg(
        F.grouping("move_type").alias("is_total_move_type"),
        F.round(F.stddev("qty"), 2).alias("stddev_qty"),
        F.min("moved_at").alias("first_moved_at"),
    )
)
