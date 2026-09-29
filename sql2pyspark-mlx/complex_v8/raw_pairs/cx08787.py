from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter((F.col("category") != "Toys") & (F.col("unit_cost") >= 158.7))
    .select("id")
)

move_rows = df_stock_moves.select("id")

result = (
    sku_side
    .intersect(move_rows)
    .orderBy(F.col("id").desc())
)
