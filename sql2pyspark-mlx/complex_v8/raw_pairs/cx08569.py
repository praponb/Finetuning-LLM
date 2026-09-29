from pyspark.sql import functions as F

move_side = df_stock_moves.select("sku_id")

warehouse_rows = (
    df_warehouses
    .filter((F.col("city") != "Singapore") & (F.col("capacity") >= 4912))
    .select("capacity")
)

result = (
    move_side
    .exceptAll(warehouse_rows)
    .orderBy("sku_id")
)
