from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter((F.col("weight_kg").between(8.5, 18.5)) & (F.col("unit_cost") > 158.7))
    .select("id")
)

warehouse_side = (
    df_warehouses
    .filter(F.col("city") != "Bangkok")
    .select("id")
)

result = (
    sku_side
    .intersect(warehouse_side)
    .orderBy("id")
)
