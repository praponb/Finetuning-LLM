from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("C%")) & (F.col("opened_on") >= F.lit("2024-11-25").cast("date")))
    .select("capacity", "opened_on")
)

warehouse_side_2 = df_warehouses.select("id", "opened_on")

combined = (
    warehouse_side
    .union(warehouse_side_2)
    .distinct()
)

result = (
    combined
    .groupBy("capacity")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("opened_on").alias("latest_opened_on"),
    )
    .orderBy("capacity")
)
