from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter((F.col("capacity") < 11768) & (F.upper(F.trim(F.col("wh_code"))).like("K%")))
    .select(F.col("capacity").alias("key_id"))
)

warehouse_side = df_warehouses.select(F.col("id").alias("key_id"))

result = (
    warehouse_rows
    .exceptAll(warehouse_side)
    .orderBy(F.col("key_id").desc())
)
