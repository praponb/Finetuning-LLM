from pyspark.sql import functions as F

sku_side = (
    df_skus
    .filter((F.col("unit_cost") < 306.4) & (F.array_contains(F.col("labels"), "fragile")))
    .select("id")
)

warehouse_side = df_warehouses.select("id")

result = (
    sku_side
    .intersectAll(warehouse_side)
    .orderBy("id")
)
