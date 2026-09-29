from pyspark.sql import functions as F

sku_side = df_skus.select(F.col("id").alias("entity_id"))

warehouse_side = df_warehouses.select(F.col("capacity").alias("entity_id"))

result = sku_side.subtract(warehouse_side)
