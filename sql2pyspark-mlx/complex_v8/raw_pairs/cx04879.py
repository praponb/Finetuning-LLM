from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.col("warehouse_id") <= 3)
    .select(
        F.col("id").alias("ref_id"),
        F.col("sku_id").alias("qty"),
        F.col("operator").alias("name"),
    )
)

warehouse_side = df_warehouses.select("capacity", "id", "city")

result = (
    move_rows
    .union(warehouse_side)
    .orderBy("ref_id")
)
