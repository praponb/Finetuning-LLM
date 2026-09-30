from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter((F.col("unit_price") > 164.1) & (F.col("qty") >= 67))
    .select(
        F.col("move_type").alias("label"),
        F.col("moved_at").alias("happened_at"),
    )
)

move_side_2 = (
    df_stock_moves
    .select(
        F.col("operator").alias("label"),
        F.col("moved_at").alias("happened_at"),
    )
)

result = move_side.union(move_side_2)
