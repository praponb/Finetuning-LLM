from pyspark.sql import functions as F

sku_rows = (
    df_skus
    .filter((F.col("sku_code").isNotNull()) & (F.col("is_active")))
    .select(F.col("category").alias("name"))
)

warehouse_side = (
    df_warehouses
    .filter((F.col("capacity") < 11506) & (F.col("wh_code").isNotNull()))
    .select(F.col("city").alias("name"))
)

result = (
    sku_rows
    .exceptAll(warehouse_side)
    .orderBy(F.col("name").desc())
)
