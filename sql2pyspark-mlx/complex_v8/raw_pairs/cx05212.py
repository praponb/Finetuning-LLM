from pyspark.sql import functions as F

base_warehouses = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("C%")) & (~F.col("city").isin("Kuala Lumpur", "Singapore")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

warehouse_rows = base_warehouses.select("capacity")

warehouse_side = df_warehouses.select("capacity")

result = (
    warehouse_rows
    .subtract(warehouse_side)
    .orderBy("capacity")
)
