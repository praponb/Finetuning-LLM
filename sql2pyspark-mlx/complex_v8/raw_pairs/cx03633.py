from pyspark.sql import functions as F

valid_moves = df_stock_moves

move_side = (
    valid_moves
    .filter((F.col("operator").isin("op_eve", "op_dew", "op_bee")) & (F.col("unit_price") > 125.9))
    .select(
        F.col("operator").alias("label"),
        "unit_price",
        "moved_at",
        F.lit("move").alias("origin"),
    )
)

move_rows = (
    df_stock_moves
    .select(
        F.col("operator").alias("label"),
        "unit_price",
        "moved_at",
        F.lit("move_2").alias("origin"),
    )
)

result = (
    move_side
    .union(move_rows)
    .orderBy("label")
)
