from pyspark.sql import functions as F

sku_rows = (
    df_skus
    .filter(F.col("category") != "Toys")
    .select("category")
)

move_side = df_stock_moves.select(F.col("operator").alias("category"))

result = (
    sku_rows
    .subtract(move_side)
    .orderBy("category")
)
