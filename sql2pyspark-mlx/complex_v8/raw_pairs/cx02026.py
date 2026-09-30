from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .select(F.col("id").alias("qty"), "capacity", F.lit("warehouse").alias("origin"))
)

warehouse_side_2 = df_warehouses.select("capacity", "id", F.lit("warehouse_2").alias("origin"))

result = warehouse_side.union(warehouse_side_2)
