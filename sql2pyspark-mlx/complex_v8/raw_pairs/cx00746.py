from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 3)
    .select(
        F.lit("move").alias("origin"),
        "operator",
        F.col("move_type").alias("name"),
    )
)

move_rows = (
    df_stock_moves
    .filter(F.col("unit_price") <= 330.9)
    .select(F.lit("move_2").alias("origin"), "move_type", "operator")
)

move_side_2 = df_stock_moves.select(F.lit("move_3").alias("origin"), "move_type", "operator")

result = (
    move_side
    .union(move_rows)
    .union(move_side_2)
)
