from pyspark.sql import functions as F

warehouse_rows = df_warehouses.select("id", "opened_on")

warehouse_side = df_warehouses.select("capacity", "opened_on")

result = (
    warehouse_rows
    .union(warehouse_side)
    .distinct()
    .orderBy("id")
)
