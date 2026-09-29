from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter((F.col("sku_id") >= 18) & (F.col("qty").between(89, 423)))
    .select(F.col("qty").alias("key_id"), "moved_at", "move_type")
)

move_side_2 = df_stock_moves.select("sku_id", "moved_at", "move_type")

move_side_3 = df_stock_moves.select("id", "moved_at", "operator")

all_events = (
    move_side
    .union(move_side_2)
    .union(move_side_3)
)

result = (
    all_events
    .groupBy("move_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("key_id").alias("total_key_id"),
        F.max("moved_at").alias("latest_moved_at"),
    )
    .orderBy("move_type")
)
