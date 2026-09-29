from pyspark.sql import functions as F

sku_side = df_skus.select("id", "category", F.lit("sku").alias("origin"))

warehouse_side = (
    df_warehouses
    .filter(F.lower(F.col("wh_code")).like("%kk%"))
    .select("id", "wh_code", F.lit("warehouse").alias("origin"))
)

sku_side_2 = (
    df_skus
    .filter(F.size(F.col("labels")) >= 1)
    .select("id", "sku_code", F.lit("sku_3").alias("origin"))
)

result = (
    sku_side
    .union(warehouse_side)
    .union(sku_side_2)
    .orderBy(F.col("category").desc(), "id")
)
