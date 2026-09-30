from pyspark.sql import functions as F

sku_rows = (
    df_skus
    .filter(F.col("unit_cost") >= 134.3)
    .select(F.col("sku_code").alias("category"))
)

warehouse_rows = (
    df_warehouses
    .filter(F.col("opened_on") < F.lit("2025-07-03").cast("date"))
    .select(F.col("wh_code").alias("category"))
)

result = (
    sku_rows
    .subtract(warehouse_rows)
    .orderBy(F.col("category").desc())
)
