from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        F.col("warehouse_id").alias("key_id"),
        F.col("moved_at").alias("happened_at"),
    )
)

move_side_2 = (
    df_stock_moves
    .filter(F.col("move_type") == "ADJUST")
    .select("qty", "moved_at")
)

all_events = move_side.union(move_side_2)

result = (
    all_events
    .groupBy("key_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("key_id")
)
