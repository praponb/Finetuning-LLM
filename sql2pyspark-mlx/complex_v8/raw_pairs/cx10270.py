from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter(F.col("capacity") <= 11506)
    .select("capacity", "id", F.col("opened_on").alias("event_date"))
)

warehouse_side = df_warehouses.select("capacity", "id", F.col("opened_on").alias("event_date"))

result = warehouse_rows.unionByName(warehouse_side)
