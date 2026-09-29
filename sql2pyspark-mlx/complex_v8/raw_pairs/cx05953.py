from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.col("opened_on") < F.lit("2025-07-03").cast("date")) & (F.col("capacity") >= 2614))
    .select(
        F.col("city").alias("name"),
        F.col("capacity").alias("ref_id"),
        "opened_on",
        F.lit("warehouse").alias("src"),
    )
)

warehouse_rows = (
    df_warehouses
    .select(
        "city",
        "id",
        "opened_on",
        F.lit("warehouse_2").alias("src"),
    )
)

warehouse_side_2 = (
    df_warehouses
    .filter((F.col("city") != "Bangkok") & (F.col("opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-12-17").cast("date"))))
    .select(
        "city",
        "capacity",
        "opened_on",
        F.lit("warehouse_3").alias("src"),
    )
)

result = (
    warehouse_side
    .union(warehouse_rows)
    .union(warehouse_side_2)
)
