from pyspark.sql import functions as F

warehouse_side = df_warehouses.select("id", "city")

move_side = (
    df_stock_moves
    .filter(F.col("unit_price") >= 238.5)
    .select("qty", "move_type")
)

result = (
    warehouse_side
    .subtract(move_side)
    .orderBy(F.col("id").desc())
)
