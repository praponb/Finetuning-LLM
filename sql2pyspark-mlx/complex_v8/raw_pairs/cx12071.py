from pyspark.sql import functions as F

cleaned_warehouses = (
    df_warehouses
    .filter(F.col("opened_on") >= F.lit("2024-11-25").cast("date"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
    )
)

warehouse_rows = cleaned_warehouses.select(F.col("capacity").alias("qty"), "opened_on", "id")

warehouse_side = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("K%")) & (F.col("city") == "Kuala Lumpur"))
    .select("id", "opened_on", "capacity")
)

result = (
    warehouse_rows
    .union(warehouse_side)
    .distinct()
    .orderBy("qty", "id")
)
