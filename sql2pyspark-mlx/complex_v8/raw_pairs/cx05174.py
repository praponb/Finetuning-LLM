from pyspark.sql import functions as F

warehouse_rows = df_warehouses.select("id")

move_rows = (
    df_stock_moves
    .filter(F.col("unit_price").between(180.0, 398.5))
    .select("id")
)

result = warehouse_rows.subtract(move_rows)
