from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter((F.col("opened_on") >= F.lit("2024-11-25").cast("date")) & (F.col("city") != "Kuala Lumpur"))
    .select(
        F.col("capacity").alias("key_id"),
        F.col("opened_on").alias("event_date"),
    )
)

warehouse_rows_2 = df_warehouses.select("capacity", "opened_on")

result = (
    warehouse_rows
    .union(warehouse_rows_2)
    .orderBy("key_id", "event_date")
)
