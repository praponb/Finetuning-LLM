from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter(F.col("capacity").between(4912, 11506))
    .select(F.col("capacity").alias("entity_id"), F.col("opened_on").alias("day"))
)

warehouse_side_2 = (
    df_warehouses
    .filter(F.col("capacity").between(2614, 11506))
    .select("capacity", "opened_on")
)

result = warehouse_side.union(warehouse_side_2)
