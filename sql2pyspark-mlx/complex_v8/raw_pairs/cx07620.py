from pyspark.sql import functions as F

filtered = df_warehouses.filter(F.length(F.trim(F.col("wh_code"))) >= 7)

warehouse_side = (
    filtered
    .filter(F.col("capacity") >= 4912)
    .select("city")
)

sku_rows = df_skus.select("category")

result = (
    warehouse_side
    .exceptAll(sku_rows)
    .orderBy(F.col("city").desc())
)
