from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("move_type") == "TRANSFER") & (F.to_date(F.col("moved_at")) < F.lit("2025-03-02").cast("date")))
)

move_rows = valid_moves.select(F.col("warehouse_id").alias("qty"), "moved_at")

move_side = df_stock_moves.select(F.col("warehouse_id").alias("qty"), "moved_at")

result = (
    move_rows
    .unionByName(move_side)
    .orderBy("qty")
)
