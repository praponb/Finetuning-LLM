from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter(F.col("unit_price").between(112.8, 330.9))
    .select(F.col("id").alias("qty"), "move_type")
)

sku_side = df_skus.select(F.col("id").alias("qty"), F.col("category").alias("move_type"))

result = move_side.intersect(sku_side)
