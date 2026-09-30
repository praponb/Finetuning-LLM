from pyspark.sql import functions as F

warehouse_rows = df_warehouses.select(F.col("wh_code").alias("label"))

sku_rows = df_skus.select("sku_code")

result = warehouse_rows.subtract(sku_rows)
