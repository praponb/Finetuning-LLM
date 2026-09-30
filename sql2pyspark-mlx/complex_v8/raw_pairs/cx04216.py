from pyspark.sql import functions as F

warehouse_side = df_warehouses.select(F.col("id").alias("ref_id"), "city", "capacity")

warehouse_rows = (
    df_warehouses
    .filter((F.col("opened_on") < F.lit("2025-02-14").cast("date")) & (F.col("capacity") >= 2614))
    .select("capacity", "wh_code", "id")
)

unioned = (
    warehouse_side
    .union(warehouse_rows)
    .distinct()
)

result = (
    unioned
    .groupBy("city")
    .agg(F.count("*").alias("n_rows"), F.sum("capacity").alias("total_capacity"))
    .orderBy("city")
)
