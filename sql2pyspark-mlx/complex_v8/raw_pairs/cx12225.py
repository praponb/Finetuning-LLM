from pyspark.sql import functions as F

warehouse_side = df_warehouses.select("id", "capacity")

warehouse_side_2 = df_warehouses.select("capacity", "id")

result = (
    warehouse_side
    .union(warehouse_side_2)
    .distinct()
)
