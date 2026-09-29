from pyspark.sql import functions as F

warehouse_side = df_warehouses.select(F.col("id").alias("ref_id"), F.col("city").alias("label"))

sku_rows = df_skus.select("id", "sku_code")

result = (
    warehouse_side
    .intersectAll(sku_rows)
    .orderBy(F.col("ref_id").desc())
)
