from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .select(
        F.col("qty").alias("key_id"),
        F.col("moved_at").alias("happened_at"),
        "operator",
        F.lit("move").alias("source"),
    )
)

move_side = (
    df_stock_moves
    .filter((F.col("moved_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")) & (F.col("move_type").isNotNull()))
    .select(
        "id",
        "moved_at",
        "move_type",
        F.lit("move_2").alias("source"),
    )
)

result = move_rows.union(move_side)
