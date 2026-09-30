from pyspark.sql import functions as F

prepared = (
    df_warehouses
    .filter(F.col("opened_on").between(F.lit("2024-11-25").cast("date"), F.lit("2025-07-03").cast("date")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.abs(F.col("capacity") - 11005).alias("capacity_distance"),
    )
)

warehouse_side = (
    prepared
    .filter(F.upper(F.trim(F.col("wh_code"))).like("C%"))
    .select(F.col("capacity_distance").alias("key_id"), "city", "opened_on")
)

warehouse_rows = df_warehouses.select("capacity", "wh_code", "opened_on")

warehouse_side_2 = df_warehouses.select("id", "wh_code", "opened_on")

combined = (
    warehouse_side
    .union(warehouse_rows)
    .union(warehouse_side_2)
)

result = (
    combined
    .groupBy("city")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("key_id").alias("total_key_id"),
        F.max("opened_on").alias("latest_opened_on"),
    )
    .orderBy("city")
)
