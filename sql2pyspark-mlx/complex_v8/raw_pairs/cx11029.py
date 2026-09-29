from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.col("city") != "Singapore") & (F.lower(F.col("wh_code")).like("%cn%")))
    .select(
        F.col("wh_code").alias("category"),
        F.col("opened_on").alias("event_date"),
        F.lit("warehouse").alias("source"),
    )
)

warehouse_rows = (
    df_warehouses
    .filter(F.col("capacity") >= 11005)
    .select("wh_code", "opened_on", F.lit("warehouse_2").alias("source"))
)

combined = warehouse_side.union(warehouse_rows)

result = (
    combined
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("source")
)
