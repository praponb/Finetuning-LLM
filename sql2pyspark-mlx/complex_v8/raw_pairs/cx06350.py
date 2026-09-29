from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.length(F.trim(F.col("wh_code"))) >= 4) & (~F.col("city").isin("Bangkok", "Kuala Lumpur")))
    .select("capacity", "wh_code")
)

sku_side = (
    df_skus
    .filter((F.col("category") == "Beauty") & (F.array_contains(F.col("labels"), "express")))
    .select("id", "category")
)

result = warehouse_side.union(sku_side)
