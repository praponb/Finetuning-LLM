from pyspark.sql import functions as F

cleaned_warehouses = (
    df_warehouses
    .filter((F.col("city") == "Kuala Lumpur") & (F.col("capacity") > 4912))
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
    )
)

warehouse_rows = (
    cleaned_warehouses
    .filter((F.col("opened_on").between(F.lit("2024-02-19").cast("date"), F.lit("2025-07-03").cast("date"))) & (F.col("capacity") < 11506))
    .select(
        "id",
        "capacity",
        F.col("wh_code").alias("label"),
        F.lit("warehouse").alias("src"),
    )
)

warehouse_rows_2 = (
    df_warehouses
    .select(
        "capacity",
        "id",
        "wh_code",
        F.lit("warehouse_2").alias("src"),
    )
)

result = (
    warehouse_rows
    .union(warehouse_rows_2)
    .distinct()
    .orderBy("capacity", "id")
)
