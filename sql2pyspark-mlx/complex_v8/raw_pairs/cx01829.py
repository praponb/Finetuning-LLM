from pyspark.sql import functions as F

warehouse_rows = df_warehouses.select("id")

warehouse_side = (
    df_warehouses
    .filter((F.col("city").isNotNull()) & (F.col("opened_on").isNotNull()))
    .select("capacity")
)

result = (
    warehouse_rows
    .intersectAll(warehouse_side)
    .orderBy(F.col("id").desc())
)
