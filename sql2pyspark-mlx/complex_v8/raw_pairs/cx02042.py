from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .select(
        F.lit("warehouse").alias("source"),
        F.col("id").alias("entity_id"),
        F.col("capacity").alias("key_id"),
        "opened_on",
    )
)

warehouse_rows_2 = (
    df_warehouses
    .select(
        F.lit("warehouse_2").alias("source"),
        "id",
        "capacity",
        "opened_on",
    )
)

warehouse_rows_3 = (
    df_warehouses
    .select(
        F.lit("warehouse_3").alias("source"),
        "capacity",
        "id",
        "opened_on",
    )
)

result = (
    warehouse_rows
    .union(warehouse_rows_2)
    .union(warehouse_rows_3)
)
