from pyspark.sql import functions as F

move_side = df_stock_moves.select("qty", "unit_price")

sku_rows = (
    df_skus
    .filter(F.col("is_active"))
    .select(F.col("id").alias("qty"), F.col("unit_cost").alias("unit_price"))
)

result = move_side.unionByName(sku_rows)
