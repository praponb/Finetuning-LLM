from pyspark.sql import functions as F

move_side = df_stock_moves.select("id")

warehouse_rows = df_warehouses.select("id")

result = (
    move_side
    .exceptAll(warehouse_rows)
    .orderBy("id")
)
