from pyspark.sql import functions as F

move_rows = df_stock_moves.select("operator", "unit_price")

move_rows_2 = df_stock_moves.select("operator", "unit_price")

result = move_rows.union(move_rows_2)
