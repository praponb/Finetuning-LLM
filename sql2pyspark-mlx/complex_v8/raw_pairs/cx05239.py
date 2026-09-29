from pyspark.sql import functions as F

warehouse_rows = df_warehouses.select("city", "id")

sku_rows = df_skus.select(F.col("category").alias("city"), "id")

move_rows = (
    df_stock_moves
    .filter((F.col("sku_id") <= 19) & (F.col("move_type") == "TRANSFER"))
    .select(F.col("move_type").alias("city"), "id")
)

result = (
    warehouse_rows
    .union(sku_rows)
    .union(move_rows)
)
