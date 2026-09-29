from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.col("move_type") == "TRANSFER")
    .select(F.col("id").alias("key_id"), F.col("operator").alias("category"))
)

warehouse_rows = df_warehouses.select("capacity", "wh_code")

result = (
    move_rows
    .intersect(warehouse_rows)
    .orderBy("key_id", "category")
)
