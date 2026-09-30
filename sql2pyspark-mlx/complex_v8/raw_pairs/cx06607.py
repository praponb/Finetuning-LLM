from pyspark.sql import functions as F

move_side = df_stock_moves.select(F.col("operator").alias("category"))

sku_rows = df_skus.select("sku_code")

result = (
    move_side
    .exceptAll(sku_rows)
    .orderBy("category")
)
