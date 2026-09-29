from pyspark.sql import functions as F

scoped_warehouses = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("K%")) & (F.col("city") == "Kuala Lumpur"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

warehouse_side = scoped_warehouses.select(F.col("capacity").alias("ref_id"))

warehouse_side_2 = df_warehouses.select(F.col("capacity").alias("ref_id"))

result = (
    warehouse_side
    .subtract(warehouse_side_2)
    .orderBy("ref_id")
)
