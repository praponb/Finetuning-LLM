from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("sku_id") <= 7)
    .select("qty", "moved_at")
)

move_rows = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-03-02 12:00:00").cast("timestamp"))
    .select(F.col("id").alias("qty"), "moved_at")
)

move_rows_2 = df_stock_moves.select(F.col("id").alias("qty"), "moved_at")

result = (
    move_side
    .unionByName(move_rows)
    .unionByName(move_rows_2)
    .orderBy("moved_at")
)
