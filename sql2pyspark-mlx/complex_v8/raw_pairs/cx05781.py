from pyspark.sql import functions as F

warehouse_side = df_warehouses.select(F.col("capacity").alias("entity_id"), "wh_code")

move_side = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(F.col("qty").alias("entity_id"), F.col("move_type").alias("wh_code"))
)

result = warehouse_side.subtract(move_side)
