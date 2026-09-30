from pyspark.sql import functions as F

sku_side = df_skus.select("id", F.col("sku_code").alias("name"))

move_rows = (
    df_stock_moves
    .filter(F.col("qty").between(78, 415))
    .select("sku_id", "operator")
)

move_side = df_stock_moves.select("warehouse_id", "operator")

result = (
    sku_side
    .union(move_rows)
    .union(move_side)
)
