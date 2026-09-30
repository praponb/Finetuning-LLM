from pyspark.sql import functions as F

scoped = (
    df_warehouses
    .filter(F.lower(F.col("wh_code")).like("%bk%"))
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
    )
)

warehouse_rows = (
    scoped
    .filter((F.col("opened_on") >= F.lit("2024-11-25").cast("date")) & (F.col("city") == "Singapore"))
    .select("id", "opened_on", F.lit("warehouse").alias("origin"))
)

warehouse_side = df_warehouses.select("id", "opened_on", F.lit("warehouse_2").alias("origin"))

warehouse_side_2 = (
    df_warehouses
    .filter(F.length(F.trim(F.col("wh_code"))) >= 7)
    .select(
        F.col("capacity").alias("id"),
        "opened_on",
        F.lit("warehouse_3").alias("origin"),
    )
)

result = (
    warehouse_rows
    .unionByName(warehouse_side)
    .unionByName(warehouse_side_2)
)
