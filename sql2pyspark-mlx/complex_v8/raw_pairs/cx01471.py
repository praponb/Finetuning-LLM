from pyspark.sql import functions as F

sku_rows = (
    df_skus
    .filter(F.upper(F.trim(F.col("sku_code"))).like("E%"))
    .select("id")
)

warehouse_rows = (
    df_warehouses
    .filter((F.col("opened_on").isNotNull()) & (F.col("capacity") >= 4912))
    .select("id")
)

result = (
    sku_rows
    .subtract(warehouse_rows)
    .orderBy(F.col("id").desc())
)
