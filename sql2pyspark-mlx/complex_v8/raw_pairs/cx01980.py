from pyspark.sql import functions as F

prepared = (
    df_warehouses
    .filter(F.col("city") == "Bangkok")
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
    )
)

warehouse_side = prepared.select("capacity")

warehouse_rows = df_warehouses.select(F.col("id").alias("capacity"))

result = (
    warehouse_side
    .intersectAll(warehouse_rows)
    .orderBy("capacity")
)
