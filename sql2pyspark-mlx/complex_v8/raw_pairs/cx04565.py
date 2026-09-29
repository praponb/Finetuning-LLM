from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-13 10:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

screened = (
    valid_moves
    .crossJoin(valid_moves.agg(F.min("unit_price").alias("overall_min_unit_price")))
    .filter(F.col("unit_price") > F.col("overall_min_unit_price"))
    .drop("overall_min_unit_price")
)

grouped = (
    screened
    .groupBy("operator")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.max("unit_price").alias("max_unit_price_check"),
    )
    .filter(F.col("max_unit_price_check") > 130.6)
    .drop("max_unit_price_check")
)

result = (
    grouped
    .filter(F.month(F.col("first_moved_at")).isin(2, 3))
    .select(
        "operator",
        "distinct_operators",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_moved_at")).alias("days_since_first_moved_at"),
    )
)
