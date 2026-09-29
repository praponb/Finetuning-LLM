from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .select(
        "move_type",
        F.col("operator").alias("category"),
        F.col("unit_price").alias("metric"),
    )
)

move_side_2 = (
    df_stock_moves
    .filter(F.col("qty") >= 109)
    .select("move_type", "operator", "unit_price")
)

unioned = move_side.union(move_side_2)

result = (
    unioned
    .groupBy("move_type")
    .agg(F.count("*").alias("n_rows"), F.sum("metric").alias("total_metric"))
    .orderBy("move_type")
)
