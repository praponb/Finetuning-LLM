from pyspark.sql import functions as F

warehouse_rows = df_warehouses.select("city", "opened_on", F.lit("warehouse").alias("record_type"))

warehouse_rows_2 = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("C%"))
    .select("city", "opened_on", F.lit("warehouse_2").alias("record_type"))
)

warehouse_side = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("B%"))
    .select("wh_code", "opened_on", F.lit("warehouse_3").alias("record_type"))
)

result = (
    warehouse_rows
    .union(warehouse_rows_2)
    .union(warehouse_side)
    .orderBy("city", "record_type")
)
