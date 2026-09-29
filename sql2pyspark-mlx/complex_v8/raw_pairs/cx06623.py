from pyspark.sql import functions as F

cleaned_warehouses = (
    df_warehouses
    .filter((F.col("opened_on") >= F.lit("2024-11-25").cast("date")) & (F.col("capacity") > 2614))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

warehouse_rows = cleaned_warehouses.select(F.col("id").alias("entity_id"), "capacity", "city")

warehouse_rows_2 = (
    df_warehouses
    .filter(F.col("city") == "Bangkok")
    .select("capacity", "id", "city")
)

stacked = warehouse_rows.union(warehouse_rows_2)

result = (
    stacked
    .groupBy("city")
    .agg(F.count("*").alias("n_rows"), F.sum("capacity").alias("total_capacity"))
    .orderBy("city")
)
