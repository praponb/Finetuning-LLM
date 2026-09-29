from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("C%")) & (F.col("capacity") < 11768))
    .select(
        F.col("capacity").alias("entity_id"),
        "opened_on",
        F.lit("warehouse").alias("src"),
    )
)

warehouse_side = (
    df_warehouses
    .select(
        F.col("capacity").alias("entity_id"),
        "opened_on",
        F.lit("warehouse_2").alias("src"),
    )
)

all_events = (
    warehouse_rows
    .unionByName(warehouse_side)
    .distinct()
)

result = (
    all_events
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
        F.max("opened_on").alias("latest_opened_on"),
    )
    .orderBy("src")
)
