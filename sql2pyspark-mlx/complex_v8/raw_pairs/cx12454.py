from pyspark.sql import functions as F

move_rows = df_stock_moves.select(F.col("qty").alias("entity_id"))

sku_side = df_skus.select("id")

result = (
    move_rows
    .exceptAll(sku_side)
    .orderBy("entity_id")
)
