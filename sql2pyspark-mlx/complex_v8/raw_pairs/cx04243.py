from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-31 07:00:00").cast("timestamp"))
    .select(
        "sku_id",
        F.col("moved_at").alias("happened_at"),
        F.lit("move").alias("origin"),
    )
)

move_side = df_stock_moves.select("warehouse_id", "moved_at", F.lit("move_2").alias("origin"))

move_side_2 = df_stock_moves.select("id", "moved_at", F.lit("move_3").alias("origin"))

result = (
    move_rows
    .union(move_side)
    .union(move_side_2)
)
