from pyspark.sql import functions as F

move_side = df_stock_moves.select("warehouse_id", "moved_at", F.col("move_type").alias("name"))

move_side_2 = df_stock_moves.select("id", "moved_at", "operator")

result = (
    move_side
    .union(move_side_2)
    .orderBy(F.col("warehouse_id").desc())
)
