from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .select(F.lit("warehouse").alias("src"), F.col("id").alias("key_id"), "city")
)

sku_rows = (
    df_skus
    .filter(~F.col("is_active"))
    .select(F.lit("sku").alias("src"), "id", "sku_code")
)

result = (
    warehouse_side
    .union(sku_rows)
    .orderBy("city")
)
