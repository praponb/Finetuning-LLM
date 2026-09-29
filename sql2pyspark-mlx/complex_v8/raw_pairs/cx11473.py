from pyspark.sql import functions as F

sku_side = df_skus.select(F.col("id").alias("ref_id"), F.col("category").alias("name"))

move_rows = df_stock_moves.select("id", "move_type")

result = sku_side.subtract(move_rows)
