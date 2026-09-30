from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter(F.col("capacity") < 11768)
    .select(
        F.lit("warehouse").alias("record_type"),
        "city",
        "capacity",
        F.col("opened_on").alias("day"),
    )
)

warehouse_side = (
    df_warehouses
    .filter(F.col("opened_on") >= F.lit("2024-11-25").cast("date"))
    .select(
        F.lit("warehouse_2").alias("record_type"),
        "city",
        "id",
        "opened_on",
    )
)

warehouse_rows_2 = (
    df_warehouses
    .filter((F.col("city").isin("Kuala Lumpur", "Bangkok")) & (F.lower(F.col("wh_code")).like("%nx%")))
    .select(
        F.lit("warehouse_3").alias("record_type"),
        "wh_code",
        "capacity",
        "opened_on",
    )
)

result = (
    warehouse_rows
    .union(warehouse_side)
    .union(warehouse_rows_2)
)
