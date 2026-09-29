from pyspark.sql import functions as F

sku_rows = (
    df_skus
    .filter(F.col("unit_cost") > 38.3)
    .select(F.col("id").alias("entity_id"))
)

warehouse_side = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("B%")) & (F.col("city") == "Singapore"))
    .select(F.col("id").alias("entity_id"))
)

result = (
    sku_rows
    .exceptAll(warehouse_side)
    .orderBy(F.col("entity_id").desc())
)
