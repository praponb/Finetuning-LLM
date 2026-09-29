from pyspark.sql import functions as F

warehouse_side = df_warehouses.select(F.col("capacity").alias("entity_id"), "wh_code")

warehouse_rows = (
    df_warehouses
    .filter(F.col("opened_on") < F.lit("2025-02-14").cast("date"))
    .select("capacity", "city")
)

result = warehouse_side.intersect(warehouse_rows)
