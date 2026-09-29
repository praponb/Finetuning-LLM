from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .select(
        "move_type",
        F.col("warehouse_id").alias("ref_id"),
        F.col("moved_at").alias("event_time"),
    )
)

move_side = (
    df_stock_moves
    .select(
        F.col("operator").alias("move_type"),
        F.col("warehouse_id").alias("ref_id"),
        F.col("moved_at").alias("event_time"),
    )
)

result = move_rows.unionByName(move_side)
