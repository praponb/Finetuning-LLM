from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("qty") <= 288)
    .select("id", "moved_at")
)

move_side_2 = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp"))
    .select("qty", "moved_at")
)

move_rows = df_stock_moves.select("sku_id", "moved_at")

result = (
    move_side
    .union(move_side_2)
    .union(move_rows)
    .orderBy("id")
)
