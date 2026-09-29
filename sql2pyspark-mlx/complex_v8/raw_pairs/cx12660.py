from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        "operator",
        F.col("moved_at").alias("happened_at"),
        F.lit("move").alias("src"),
    )
)

move_side_2 = (
    df_stock_moves
    .select(
        F.col("move_type").alias("operator"),
        F.col("moved_at").alias("happened_at"),
        F.lit("move_2").alias("src"),
    )
)

move_side_3 = (
    df_stock_moves
    .select(
        F.col("move_type").alias("operator"),
        F.col("moved_at").alias("happened_at"),
        F.lit("move_3").alias("src"),
    )
)

result = (
    move_side
    .unionByName(move_side_2)
    .unionByName(move_side_3)
    .orderBy(F.col("src").desc())
)
