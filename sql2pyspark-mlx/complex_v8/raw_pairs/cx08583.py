from pyspark.sql import functions as F

warehouse_side = df_warehouses.select(F.lit("warehouse").alias("src"), "id", "capacity")

warehouse_side_2 = (
    df_warehouses
    .filter(F.year(F.col("opened_on")) == 2025)
    .select(F.lit("warehouse_2").alias("src"), "id", "capacity")
)

result = (
    warehouse_side
    .union(warehouse_side_2)
    .orderBy("capacity")
)
