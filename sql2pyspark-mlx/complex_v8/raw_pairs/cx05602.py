from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.col("capacity") < 11768) & (F.col("city") == "Kuala Lumpur"))
    .select("capacity")
)

warehouse_rows = df_warehouses.select("capacity")

result = warehouse_side.intersect(warehouse_rows)
