from pyspark.sql import functions as F

prepared_warehouses = df_warehouses.filter(F.col("opened_on") >= F.lit("2024-02-19").cast("date"))

warehouse_rows = (
    prepared_warehouses
    .select(F.col("id").alias("key_id"), F.col("opened_on").alias("event_date"))
)

warehouse_side = df_warehouses.select("id", "opened_on")

warehouse_side_2 = (
    df_warehouses
    .filter(~F.col("city").isin("Singapore", "Bangkok"))
    .select("id", "opened_on")
)

result = (
    warehouse_rows
    .union(warehouse_side)
    .union(warehouse_side_2)
    .orderBy("event_date", "key_id")
)
