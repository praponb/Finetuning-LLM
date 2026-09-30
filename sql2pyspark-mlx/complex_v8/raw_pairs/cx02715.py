from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter((F.col("city") != "Bangkok") & (F.col("opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-07-03").cast("date"))))
    .select(
        F.lit("warehouse").alias("src"),
        F.col("id").alias("qty"),
        F.col("city").alias("label"),
        "opened_on",
    )
)

warehouse_rows_2 = (
    df_warehouses
    .select(
        F.lit("warehouse_2").alias("src"),
        "capacity",
        "wh_code",
        "opened_on",
    )
)

combined = warehouse_rows.union(warehouse_rows_2)

result = (
    combined
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("opened_on").alias("latest_opened_on"),
    )
    .orderBy("src")
)
