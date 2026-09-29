from pyspark.sql import functions as F

warehouse_side = df_warehouses.select("id")

move_rows = df_stock_moves.select("qty")

result = (
    warehouse_side
    .exceptAll(move_rows)
    .orderBy(F.col("id").desc())
)
