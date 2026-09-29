from pyspark.sql import functions as F

filtered_warehouses = (
    df_warehouses
    .filter(F.col("city") == "Kuala Lumpur")
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
    )
)

warehouse_rows = (
    df_warehouses
    .select(
        "city",
        "opened_on",
        F.col("wh_code").alias("category"),
        F.lit("warehouse").alias("record_type"),
    )
)

warehouse_rows_2 = (
    df_warehouses
    .select(
        "wh_code",
        "opened_on",
        "city",
        F.lit("warehouse_2").alias("record_type"),
    )
)

result = (
    warehouse_rows
    .union(warehouse_rows_2)
    .orderBy("category", "opened_on")
)
