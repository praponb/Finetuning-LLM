from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-31 07:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

move_side = cleaned.select("id")

sku_rows = df_skus.select("id")

result = (
    move_side
    .intersect(sku_rows)
    .orderBy(F.col("id").desc())
)
