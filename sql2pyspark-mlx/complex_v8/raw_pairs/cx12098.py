from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("C%")) & (F.year(F.col("opened_on")) == 2025))
    .select(
        F.col("city").alias("name"),
        F.col("capacity").alias("key_id"),
        F.col("opened_on").alias("event_date"),
    )
)

warehouse_side_2 = (
    df_warehouses
    .select(
        F.col("wh_code").alias("name"),
        F.col("id").alias("key_id"),
        F.col("opened_on").alias("event_date"),
    )
)

result = (
    warehouse_side
    .union(warehouse_side_2)
    .distinct()
)
