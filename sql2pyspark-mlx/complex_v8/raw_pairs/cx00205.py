from pyspark.sql import functions as F

sku_rows = df_skus.select("category", F.col("id").alias("key_id"))

move_side = df_stock_moves.select(F.col("move_type").alias("category"), F.col("qty").alias("key_id"))

result = (
    sku_rows
    .union(move_side)
    .orderBy(F.col("category").desc())
)
