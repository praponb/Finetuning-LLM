from pyspark.sql import functions as F

warehouse_rows = df_warehouses.select("id", "wh_code")

warehouse_side = (
    df_warehouses
    .filter((F.col("opened_on") < F.lit("2025-07-03").cast("date")) & (F.col("capacity") >= 4912))
    .select("id", "city")
)

result = (
    warehouse_rows
    .exceptAll(warehouse_side)
    .orderBy(F.col("id").desc())
)
