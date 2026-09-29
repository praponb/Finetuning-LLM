from pyspark.sql import functions as F

valid = (
    df_warehouses
    .filter((F.col("opened_on") < F.lit("2025-07-03").cast("date")) & (F.col("city") != "Kuala Lumpur"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

warehouse_side = valid.select("capacity", F.col("id").alias("ref_id"))

warehouse_side_2 = (
    df_warehouses
    .filter(F.lower(F.col("wh_code")).like("%bk%"))
    .select("capacity", "id")
)

result = (
    warehouse_side
    .union(warehouse_side_2)
    .distinct()
)
