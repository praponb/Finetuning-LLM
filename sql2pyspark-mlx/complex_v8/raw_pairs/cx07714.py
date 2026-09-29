from pyspark.sql import functions as F

warehouse_rows = df_warehouses.select(F.col("capacity").alias("qty"))

sku_side = (
    df_skus
    .filter(F.col("sku_code").isNotNull())
    .select("id")
)

result = (
    warehouse_rows
    .subtract(sku_side)
    .orderBy(F.col("qty").desc())
)
