from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        F.col("operator").alias("category"),
        F.col("move_type").alias("label"),
        "unit_price",
        F.lit("move").alias("source"),
    )
)

move_rows = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        "move_type",
        "operator",
        "unit_price",
        F.lit("move_2").alias("source"),
    )
)

stacked = move_side.union(move_rows)

result = (
    stacked
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy("source")
)
