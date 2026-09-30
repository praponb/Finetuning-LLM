from pyspark.sql import functions as F

move_side = (
    df_stock_moves
    .filter((F.col("operator").isin("op_chan", "op_eve", "op_dew")) & (F.col("warehouse_id") <= 3))
    .select(
        F.col("warehouse_id").alias("entity_id"),
        F.col("id").alias("entity_id_2"),
    )
)

warehouse_side = df_warehouses.select("capacity", "id")

result = (
    move_side
    .union(warehouse_side)
    .distinct()
    .orderBy(F.col("entity_id").desc())
)
