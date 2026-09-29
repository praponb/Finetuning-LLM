from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-01-13 10:00:00").cast("timestamp")) & (F.col("qty") >= 112))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

move_side = (
    df_stock_moves
    .select(
        "qty",
        "move_type",
        "moved_at",
        F.lit("move").alias("source"),
    )
)

move_side_2 = (
    df_stock_moves
    .select(
        "id",
        "operator",
        "moved_at",
        F.lit("move_2").alias("source"),
    )
)

result = (
    move_side
    .union(move_side_2)
    .orderBy("source")
)
