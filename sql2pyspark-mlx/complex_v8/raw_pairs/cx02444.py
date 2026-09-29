from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-06").cast("date"))
    .select("qty", F.col("moved_at").alias("happened_at"))
)

move_side_2 = df_stock_moves.select("qty", "moved_at")

result = move_side.union(move_side_2)
