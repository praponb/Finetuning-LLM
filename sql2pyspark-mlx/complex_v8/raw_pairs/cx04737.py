from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter((F.length(F.trim(F.col("wh_code"))) >= 6) & (F.col("capacity") <= 11506))
    .select("capacity")
)

warehouse_side = df_warehouses.select("id")

result = warehouse_rows.intersect(warehouse_side)
