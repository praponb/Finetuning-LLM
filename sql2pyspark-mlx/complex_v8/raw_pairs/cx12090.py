from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .select(
        "id",
        "capacity",
        "city",
        F.lit("warehouse").alias("record_type"),
    )
)

warehouse_side_2 = (
    df_warehouses
    .filter((F.col("city") == "Kuala Lumpur") & (F.col("capacity").between(2614, 11506)))
    .select(
        "capacity",
        "id",
        "city",
        F.lit("warehouse_2").alias("record_type"),
    )
)

result = warehouse_side.union(warehouse_side_2)
