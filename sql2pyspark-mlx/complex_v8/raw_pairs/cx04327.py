from pyspark.sql import functions as F

scoped_skus = df_skus

sku_rows = scoped_skus.select(F.col("id").alias("entity_id"), "sku_code")

warehouse_rows = df_warehouses.select("capacity", "city")

result = (
    sku_rows
    .union(warehouse_rows)
    .distinct()
)
