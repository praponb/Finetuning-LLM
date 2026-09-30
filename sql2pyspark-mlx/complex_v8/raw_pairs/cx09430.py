from pyspark.sql import functions as F

move_side = df_stock_moves.select("sku_id")

move_rows = (
    df_stock_moves
    .filter((F.month(F.col("moved_at")).isin(2, 3)) & (F.col("qty") < 288))
    .select("sku_id")
)

result = move_side.subtract(move_rows)
