from pyspark.sql import functions as F

filtered = (
    df_warehouses
    .filter(F.lower(F.col("wh_code")).like("%bk%"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.concat_ws("|", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
    )
)

warehouse_side = (
    filtered
    .select(F.col("id").alias("entity_id"), F.col("opened_on").alias("event_date"))
)

warehouse_side_2 = (
    df_warehouses
    .filter(F.col("city") == "Singapore")
    .select(
        F.col("capacity").alias("entity_id"),
        F.col("opened_on").alias("event_date"),
    )
)

warehouse_side_3 = (
    df_warehouses
    .select(F.col("id").alias("entity_id"), F.col("opened_on").alias("event_date"))
)

combined = (
    warehouse_side
    .union(warehouse_side_2)
    .union(warehouse_side_3)
)

result = (
    combined
    .groupBy("entity_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("entity_id")
)
