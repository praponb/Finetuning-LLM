from pyspark.sql import functions as F

warehouse_side = df_warehouses.select(F.col("id").alias("ref_id"), "city")

warehouse_side_2 = (
    df_warehouses
    .filter((F.col("city") != "Singapore") & (F.length(F.trim(F.col("wh_code"))) >= 8))
    .select("id", "city")
)

warehouse_rows = df_warehouses.select("capacity", "wh_code")

result = (
    warehouse_side
    .union(warehouse_side_2)
    .union(warehouse_rows)
)
