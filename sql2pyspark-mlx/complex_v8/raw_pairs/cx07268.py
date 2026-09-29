from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .select(
        F.col("operator").alias("name"),
        F.col("move_type").alias("label"),
        F.col("moved_at").alias("event_time"),
        F.lit("move").alias("src"),
    )
)

move_side = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-02-06 07:00:00").cast("timestamp")) & (F.col("qty") >= 138))
    .select(
        "operator",
        "move_type",
        "moved_at",
        F.lit("move_2").alias("src"),
    )
)

move_side_2 = (
    df_stock_moves
    .select(
        "operator",
        "move_type",
        "moved_at",
        F.lit("move_3").alias("src"),
    )
)

result = (
    move_rows
    .union(move_side)
    .union(move_side_2)
)
