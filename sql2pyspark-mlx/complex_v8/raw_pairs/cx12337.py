from pyspark.sql import functions as F

move_side = df_stock_moves.select(F.col("move_type").alias("name"), "warehouse_id")

warehouse_side = df_warehouses.select("wh_code", "id")

result = move_side.union(warehouse_side)
