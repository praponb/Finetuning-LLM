from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter(~F.col("city").isin("Singapore", "Kuala Lumpur"))
    .select("capacity", "city")
)

sku_side = df_skus.select(F.col("id").alias("capacity"), F.col("category").alias("city"))

result = warehouse_side.subtract(sku_side)
