from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .select(F.col("id").alias("ref_id"), F.col("capacity").alias("ref_id_2"))
)

move_side = df_stock_moves.select("warehouse_id", "sku_id")

warehouse_side = (
    df_warehouses
    .filter((~F.col("city").isin("Bangkok", "Kuala Lumpur")) & (F.col("opened_on") >= F.lit("2024-04-13").cast("date")))
    .select("capacity", "id")
)

result = (
    warehouse_rows
    .union(move_side)
    .union(warehouse_side)
    .orderBy("ref_id")
)
