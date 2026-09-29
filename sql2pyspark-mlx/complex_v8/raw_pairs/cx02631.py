from pyspark.sql import functions as F

base = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) | (F.col("moved_at") < F.lit("2025-02-18 08:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
    )
)

move_rows = base.select(F.col("id").alias("key_id"), F.col("move_type").alias("label"))

move_side = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-31 07:00:00").cast("timestamp"))
    .select("id", "move_type")
)

result = (
    move_rows
    .exceptAll(move_side)
    .orderBy("label", "key_id")
)
