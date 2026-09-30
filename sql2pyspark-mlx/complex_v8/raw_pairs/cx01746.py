from pyspark.sql import functions as F

sku_rows = (
    df_skus
    .filter((F.lower(F.col("sku_code")).like("%-4%")) & (F.col("unit_cost").between(38.3, 330.6)))
    .select(F.col("category").alias("label"), F.col("id").alias("ref_id"))
)

warehouse_side = df_warehouses.select("wh_code", "capacity")

result = (
    sku_rows
    .union(warehouse_side)
    .distinct()
    .orderBy("ref_id")
)
