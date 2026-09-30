from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("K%")) & (F.col("opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-07-03").cast("date"))))
    .select("id", "city")
)

warehouse_rows = (
    df_warehouses
    .filter(F.col("city") != "Kuala Lumpur")
    .select("id", "city")
)

result = (
    warehouse_side
    .intersect(warehouse_rows)
    .orderBy(F.col("id").desc())
)
