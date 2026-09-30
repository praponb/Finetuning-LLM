from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        F.col("operator").alias("name"),
        F.col("moved_at").alias("event_time"),
        F.col("move_type").alias("name_2"),
    )
)

move_side_2 = df_stock_moves.select("operator", "moved_at", "move_type")

result = (
    move_side
    .union(move_side_2)
    .distinct()
)
