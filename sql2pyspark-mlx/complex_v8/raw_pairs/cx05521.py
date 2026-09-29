from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.col("city").isin("Singapore", "Kuala Lumpur")) & (F.upper(F.trim(F.col("wh_code"))).like("K%")))
    .select(F.col("city").alias("category"))
)

sku_rows = df_skus.select("category")

result = (
    warehouse_side
    .intersect(sku_rows)
    .orderBy(F.col("category").desc())
)
