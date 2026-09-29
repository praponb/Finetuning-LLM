from pyspark.sql import functions as F

valid_skus = df_skus.filter(F.col("category").isin("Food", "Beauty", "Toys"))

sku_side = valid_skus.select("category", "unit_cost")

move_side = (
    df_stock_moves
    .filter(F.col("operator").isNotNull())
    .select("operator", "unit_price")
)

result = sku_side.union(move_side)
