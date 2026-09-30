from pyspark.sql import functions as F

move_side = df_stock_moves.select("move_type", "moved_at")

move_side_2 = df_stock_moves.select(F.col("operator").alias("move_type"), "moved_at")

all_events = move_side.unionByName(move_side_2)

result = (
    all_events
    .groupBy("move_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("moved_at").alias("latest_moved_at"),
    )
    .orderBy("move_type")
)
