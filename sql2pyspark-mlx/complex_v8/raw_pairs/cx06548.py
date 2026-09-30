from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter(F.col("unit_price") <= 289.7)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

labeled = (
    filtered
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "qty_per_unit_price",
        F.round(F.col("qty_per_unit_price") * 1.5, 2).alias("qty_per_unit_price_adj"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.abs(F.col("days_since_moved_at") - 490).alias("days_since_moved_at_distance"),
    )
)

result = (
    labeled
    .select(
        "id",
        F.col("qty_per_unit_price_adj").alias("m_qty_per_unit_price_adj"),
        F.col("unit_price").alias("m_unit_price"),
    )
    .unpivot(
        ids=["id"],
        values=["m_qty_per_unit_price_adj", "m_unit_price"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
