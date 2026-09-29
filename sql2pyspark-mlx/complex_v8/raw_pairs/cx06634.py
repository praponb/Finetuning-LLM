from pyspark.sql import functions as F

move_rows = df_stock_moves.select("operator")

move_side = (
    df_stock_moves
    .filter((F.col("unit_price").between(124.1, 398.5)) & (F.col("sku_id").isNotNull()))
    .select("operator")
)

result = move_rows.subtract(move_side)
