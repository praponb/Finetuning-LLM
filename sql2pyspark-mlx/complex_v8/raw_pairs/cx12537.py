from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter(F.col("city").isNotNull())
    .select(F.lit("warehouse").alias("src"), F.col("id").alias("ref_id"), "wh_code")
)

warehouse_rows = (
    df_warehouses
    .filter((F.col("capacity") >= 4912) & (F.length(F.trim(F.col("wh_code"))) >= 5))
    .select(F.lit("warehouse_2").alias("src"), "capacity", "wh_code")
)

warehouse_rows_2 = df_warehouses.select(F.lit("warehouse_3").alias("src"), "id", "city")

result = (
    warehouse_side
    .union(warehouse_rows)
    .union(warehouse_rows_2)
    .orderBy("ref_id")
)
