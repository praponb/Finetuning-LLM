from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter(F.col("opened_on") >= F.lit("2024-02-19").cast("date"))
    .select("capacity")
)

warehouse_rows = (
    df_warehouses
    .filter(F.col("city") != "Bangkok")
    .select("id")
)

result = (
    warehouse_side
    .subtract(warehouse_rows)
    .orderBy("capacity")
)
