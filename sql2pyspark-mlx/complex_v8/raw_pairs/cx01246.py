from pyspark.sql import functions as F

move_rows = (
    df_stock_moves
    .filter(F.col("unit_price") >= 205.1)
    .select("id", F.col("sku_id").alias("entity_id"))
)

warehouse_rows = (
    df_warehouses
    .filter(F.col("capacity") <= 11506)
    .select("id", F.col("capacity").alias("entity_id"))
)

move_rows_2 = (
    df_stock_moves
    .filter((F.col("move_type") != "TRANSFER") & (F.col("unit_price") <= 277.6))
    .select(F.col("sku_id").alias("id"), F.col("id").alias("entity_id"))
)

result = (
    move_rows
    .unionByName(warehouse_rows)
    .unionByName(move_rows_2)
    .orderBy("id", "entity_id")
)
