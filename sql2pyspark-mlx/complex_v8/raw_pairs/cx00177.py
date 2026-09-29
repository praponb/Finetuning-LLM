from pyspark.sql import functions as F

filtered_moves = df_stock_moves

move_side = (
    filtered_moves
    .filter(F.col("sku_id").isNotNull())
    .select(F.col("sku_id").alias("ref_id"), F.col("move_type").alias("category"))
)

move_side_2 = (
    df_stock_moves
    .filter(F.col("qty") < 436)
    .select("id", "operator")
)

warehouse_side = (
    df_warehouses
    .filter(F.col("capacity") > 4912)
    .select("capacity", "wh_code")
)

result = (
    move_side
    .union(move_side_2)
    .union(warehouse_side)
)
