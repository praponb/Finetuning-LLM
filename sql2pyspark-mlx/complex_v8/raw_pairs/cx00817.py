from pyspark.sql import functions as F

warehouse_side = df_warehouses.select(F.col("capacity").alias("ref_id"), "city")

sku_side = (
    df_skus
    .filter(F.length(F.trim(F.col("sku_code"))) >= 4)
    .select(F.col("id").alias("ref_id"), F.col("category").alias("city"))
)

result = (
    warehouse_side
    .subtract(sku_side)
    .orderBy(F.col("city").desc())
)
