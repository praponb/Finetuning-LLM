from pyspark.sql import functions as F

warehouse_rows = df_warehouses.select("id", F.col("opened_on").alias("event_date"))

warehouse_side = (
    df_warehouses
    .select(F.col("capacity").alias("id"), F.col("opened_on").alias("event_date"))
)

warehouse_side_2 = (
    df_warehouses
    .filter(F.col("opened_on") >= F.lit("2024-04-13").cast("date"))
    .select("id", F.col("opened_on").alias("event_date"))
)

result = (
    warehouse_rows
    .unionByName(warehouse_side)
    .unionByName(warehouse_side_2)
    .orderBy("id", "event_date")
)
