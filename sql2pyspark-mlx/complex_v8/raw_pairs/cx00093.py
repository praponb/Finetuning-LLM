from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter((F.col("city").isin("Kuala Lumpur", "Bangkok")) & (F.year(F.col("opened_on")) == 2025))
    .select(F.col("id").alias("entity_id"))
)

warehouse_side = df_warehouses.select("id")

result = warehouse_rows.exceptAll(warehouse_side)
