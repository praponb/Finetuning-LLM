from pyspark.sql import functions as F

sku_side = df_skus.select(F.col("id").alias("ref_id"))

move_side = df_stock_moves.select(F.col("qty").alias("ref_id"))

result = (
    sku_side
    .intersectAll(move_side)
    .orderBy("ref_id")
)
