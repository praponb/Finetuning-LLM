from pyspark.sql import functions as F

move_side = df_stock_moves.select("warehouse_id")

warehouse_side = (
    df_warehouses
    .filter(F.col("capacity") >= 11005)
    .select(F.col("capacity").alias("warehouse_id"))
)

result = (
    move_side
    .intersectAll(warehouse_side)
    .orderBy(F.col("warehouse_id").desc())
)
