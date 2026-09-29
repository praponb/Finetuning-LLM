from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter(F.col("capacity") <= 11506)
    .select(F.col("capacity").alias("ref_id"), "opened_on")
)

warehouse_rows_2 = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("K%")) & (F.col("city") != "Bangkok"))
    .select("id", "opened_on")
)

result = warehouse_rows.union(warehouse_rows_2)
