from pyspark.sql import functions as F

warehouse_side = df_warehouses.select("id")

move_rows = (
    df_stock_moves
    .filter(F.col("sku_id") <= 4)
    .select("id")
)

result = (
    warehouse_side
    .subtract(move_rows)
    .orderBy("id")
)
