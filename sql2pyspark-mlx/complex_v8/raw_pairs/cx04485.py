from pyspark.sql import functions as F

sku_side = df_skus.select("sku_code")

warehouse_side = df_warehouses.select(F.col("wh_code").alias("sku_code"))

result = (
    sku_side
    .intersectAll(warehouse_side)
    .orderBy("sku_code")
)
