from pyspark.sql import functions as F

move_side = df_stock_moves.select("warehouse_id", "qty", F.col("operator").alias("name"))

move_side_2 = (
    df_stock_moves
    .select("warehouse_id", F.col("id").alias("qty"), F.col("operator").alias("name"))
)

result = move_side.unionByName(move_side_2)
