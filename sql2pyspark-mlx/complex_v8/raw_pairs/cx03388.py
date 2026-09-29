from pyspark.sql import functions as F

sku_side = df_skus.select(F.col("id").alias("qty"), F.col("sku_code").alias("category"))

warehouse_side = (
    df_warehouses
    .select(F.col("capacity").alias("qty"), F.col("wh_code").alias("category"))
)

result = (
    sku_side
    .subtract(warehouse_side)
    .orderBy("qty")
)
