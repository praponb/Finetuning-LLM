from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((~F.col("city").isin("Kuala Lumpur", "Bangkok")) & (F.lower(F.col("wh_code")).like("%ku%")))
    .select("id", "city", F.col("capacity").alias("qty"))
)

warehouse_rows = df_warehouses.select(F.col("capacity").alias("id"), "city", F.col("id").alias("qty"))

result = (
    warehouse_side
    .unionByName(warehouse_rows)
    .distinct()
)
