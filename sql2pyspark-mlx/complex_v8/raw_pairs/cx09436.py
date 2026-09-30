from pyspark.sql import functions as F

warehouse_side = df_warehouses.select(F.lit("warehouse").alias("src"), "id", "capacity")

warehouse_side_2 = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("B%"))
    .select(F.lit("warehouse_2").alias("src"), "id", "capacity")
)

result = (
    warehouse_side
    .unionByName(warehouse_side_2)
    .distinct()
    .orderBy("id")
)
