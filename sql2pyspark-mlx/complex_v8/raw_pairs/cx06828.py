from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.col("opened_on") < F.lit("2025-02-14").cast("date")) & (F.col("city").isin("Kuala Lumpur", "Bangkok")))
    .select(F.col("id").alias("qty"))
)

sku_rows = df_skus.select(F.col("id").alias("qty"))

result = (
    warehouse_side
    .intersect(sku_rows)
    .orderBy("qty")
)
