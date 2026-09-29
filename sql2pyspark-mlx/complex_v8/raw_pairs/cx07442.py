from pyspark.sql import functions as F

sku_side = df_skus.select(F.col("id").alias("qty"))

warehouse_rows = (
    df_warehouses
    .filter(F.year(F.col("opened_on")) == 2024)
    .select("capacity")
)

result = (
    sku_side
    .subtract(warehouse_rows)
    .orderBy("qty")
)
