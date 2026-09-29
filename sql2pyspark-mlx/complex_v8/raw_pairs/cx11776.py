from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("unit_price") >= 119.5)
    .select(F.col("sku_id").alias("qty"), "qty", F.lit("move").alias("source"))
)

move_rows = df_stock_moves.select("qty", F.col("id").alias("qty"), F.lit("move_2").alias("source"))

result = (
    move_side
    .union(move_rows)
    .orderBy("source")
)
