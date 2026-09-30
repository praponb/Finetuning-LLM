from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter((F.col("capacity") > 4912) & (F.lower(F.col("wh_code")).like("%ul%")))
    .select("id", "city")
)

warehouse_side = (
    df_warehouses
    .filter((F.length(F.trim(F.col("wh_code"))) >= 8) & (F.col("city") == "Singapore"))
    .select("capacity", "wh_code")
)

result = warehouse_rows.subtract(warehouse_side)
