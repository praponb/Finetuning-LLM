from pyspark.sql import functions as F

warehouse_side = df_warehouses.select(F.col("capacity").alias("entity_id"))

move_rows = (
    df_stock_moves
    .filter((F.col("unit_price") > 136.0) & (F.col("operator").isNotNull()))
    .select(F.col("sku_id").alias("entity_id"))
)

result = (
    warehouse_side
    .exceptAll(move_rows)
    .orderBy("entity_id")
)
