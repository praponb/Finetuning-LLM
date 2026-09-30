from pyspark.sql import functions as F

base = (
    df_warehouses
    .filter(F.col("opened_on") < F.lit("2025-07-03").cast("date"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

warehouse_side = base.select("city_or_unknown", "id", F.col("opened_on").alias("day"))

warehouse_rows = df_warehouses.select("city", "id", "opened_on")

result = (
    warehouse_side
    .union(warehouse_rows)
    .distinct()
    .orderBy("city_or_unknown")
)
