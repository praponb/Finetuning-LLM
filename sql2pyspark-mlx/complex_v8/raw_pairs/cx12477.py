from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter((F.col("city").isNotNull()) & (F.upper(F.trim(F.col("wh_code"))).like("K%")))
    .select(F.lit("warehouse").alias("src"), "id", F.col("opened_on").alias("day"))
)

warehouse_side = df_warehouses.select(F.lit("warehouse_2").alias("src"), "id", "opened_on")

warehouse_rows_2 = df_warehouses.select(F.lit("warehouse_3").alias("src"), "capacity", "opened_on")

result = (
    warehouse_rows
    .union(warehouse_side)
    .union(warehouse_rows_2)
    .orderBy("src")
)
