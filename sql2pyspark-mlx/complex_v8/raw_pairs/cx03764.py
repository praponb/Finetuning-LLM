from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter(F.col("city") == "Singapore")
    .select(
        F.col("wh_code").alias("name"),
        F.col("opened_on").alias("event_date"),
        F.col("id").alias("entity_id"),
    )
)

warehouse_side_2 = (
    df_warehouses
    .filter(F.col("city") == "Kuala Lumpur")
    .select(
        F.col("wh_code").alias("name"),
        F.col("opened_on").alias("event_date"),
        F.col("id").alias("entity_id"),
    )
)

result = (
    warehouse_side
    .unionByName(warehouse_side_2)
    .orderBy(F.col("entity_id").desc(), "event_date")
)
