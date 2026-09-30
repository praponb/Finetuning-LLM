from pyspark.sql import functions as F

cleaned_warehouses = (
    df_warehouses
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
    )
)

warehouse_rows = cleaned_warehouses.select("id")

warehouse_rows_2 = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("B%")) & (F.col("city").isNotNull()))
    .select("id")
)

result = (
    warehouse_rows
    .intersect(warehouse_rows_2)
    .orderBy(F.col("id").desc())
)
