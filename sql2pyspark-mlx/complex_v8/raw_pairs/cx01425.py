from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter((F.col("warehouse_id") >= 3) & (F.col("sku_id").isNotNull()))
    .select("warehouse_id", F.col("unit_price").alias("amount"))
)

sku_rows = (
    df_skus
    .filter(F.col("is_active"))
    .select("id", "weight_kg")
)

result = move_rows.union(sku_rows)
