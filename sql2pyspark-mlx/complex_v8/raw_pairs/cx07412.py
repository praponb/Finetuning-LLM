from pyspark.sql import functions as F

warehouse_side = df_warehouses.select(F.col("id").alias("qty"))

warehouse_rows = df_warehouses.select(F.col("capacity").alias("qty"))

result = (
    warehouse_side
    .exceptAll(warehouse_rows)
    .orderBy("qty")
)
