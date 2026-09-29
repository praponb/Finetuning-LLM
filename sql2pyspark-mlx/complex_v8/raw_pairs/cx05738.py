from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter(F.upper(F.trim(F.col("sku_code"))).like("A%"))
    .select("category")
)

sku_side_2 = (
    df_skus
    .filter(F.array_contains(F.col("labels"), "express"))
    .select("category")
)

result = sku_side.subtract(sku_side_2)
