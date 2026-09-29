from pyspark.sql import functions as F

move_rows = df_stock_moves.select("operator")

warehouse_side = df_warehouses.select("city")

result = move_rows.intersectAll(warehouse_side)
