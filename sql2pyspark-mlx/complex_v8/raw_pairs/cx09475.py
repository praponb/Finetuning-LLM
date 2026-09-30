from pyspark.sql import functions as F

move_side = df_stock_moves.select("qty", F.col("moved_at").alias("happened_at"))

move_rows = (
    df_stock_moves
    .select(
        F.col("warehouse_id").alias("qty"),
        F.col("moved_at").alias("happened_at"),
    )
)

result = move_side.union(move_rows)
