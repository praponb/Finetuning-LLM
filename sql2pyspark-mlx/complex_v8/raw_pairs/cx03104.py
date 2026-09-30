from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter((F.col("sku_id") <= 16) | (F.to_date(F.col("moved_at")) < F.lit("2025-03-14").cast("date")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

move_side = (
    scoped
    .select(
        F.col("qty").alias("ref_id"),
        F.col("moved_at").alias("event_time"),
        F.lit("move").alias("origin"),
    )
)

move_rows = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select("qty", "moved_at", F.lit("move_2").alias("origin"))
)

result = move_side.union(move_rows)
