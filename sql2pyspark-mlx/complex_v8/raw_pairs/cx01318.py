from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter(F.lower(F.col("wh_code")).like("%x1%"))
    .select(
        "id",
        "opened_on",
        "wh_code",
        F.lit("warehouse").alias("record_type"),
    )
)

warehouse_side_2 = (
    df_warehouses
    .select(
        "id",
        "opened_on",
        "wh_code",
        F.lit("warehouse_2").alias("record_type"),
    )
)

warehouse_rows = (
    df_warehouses
    .filter(F.col("city") == "Kuala Lumpur")
    .select(
        "id",
        "opened_on",
        "city",
        F.lit("warehouse_3").alias("record_type"),
    )
)

result = (
    warehouse_side
    .union(warehouse_side_2)
    .union(warehouse_rows)
)
