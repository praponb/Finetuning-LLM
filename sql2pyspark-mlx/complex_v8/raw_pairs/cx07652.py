from pyspark.sql import functions as F

warehouse_side = df_warehouses.select(F.col("id").alias("ref_id"), F.col("wh_code").alias("category"))

move_side = df_stock_moves.select("sku_id", "operator")

result = (
    warehouse_side
    .intersect(move_side)
    .orderBy("ref_id")
)
