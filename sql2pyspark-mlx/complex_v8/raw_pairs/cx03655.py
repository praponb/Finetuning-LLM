from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .select(
        F.lit("warehouse").alias("source"),
        "id",
        F.col("opened_on").alias("event_date"),
        F.col("capacity").alias("entity_id"),
    )
)

warehouse_rows = (
    df_warehouses
    .select(
        F.lit("warehouse_2").alias("source"),
        "id",
        "opened_on",
        "capacity",
    )
)

result = warehouse_side.union(warehouse_rows)
