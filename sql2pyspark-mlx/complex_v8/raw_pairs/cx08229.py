from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("K%"))
    .select(
        F.col("id").alias("entity_id"),
        "capacity",
        F.col("opened_on").alias("day"),
        F.lit("warehouse").alias("src"),
    )
)

warehouse_rows = (
    df_warehouses
    .filter((F.year(F.col("opened_on")) == 2024) & (F.col("capacity") > 2614))
    .select(
        "id",
        "capacity",
        "opened_on",
        F.lit("warehouse_2").alias("src"),
    )
)

warehouse_side_2 = (
    df_warehouses
    .select(
        "capacity",
        "id",
        "opened_on",
        F.lit("warehouse_3").alias("src"),
    )
)

combined = (
    warehouse_side
    .union(warehouse_rows)
    .union(warehouse_side_2)
)

result = (
    combined
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("capacity").alias("total_capacity"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("src")
)
