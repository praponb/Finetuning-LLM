from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter(F.length(F.trim(F.col("wh_code"))) >= 7)
    .select("id")
)

move_side = df_stock_moves.select("qty")

result = (
    warehouse_side
    .subtract(move_side)
    .orderBy("id")
)
