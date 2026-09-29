from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("move_type") != "OUTBOUND"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

move_side = (
    filtered_moves
    .filter((F.col("move_type") == "ADJUST") & (F.col("warehouse_id") >= 2))
    .select("qty")
)

sku_side = df_skus.select("id")

result = (
    move_side
    .subtract(sku_side)
    .orderBy(F.col("qty").desc())
)
