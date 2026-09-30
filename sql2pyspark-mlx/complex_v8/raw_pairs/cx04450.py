from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .select(
        F.col("wh_code").alias("label"),
        F.col("city").alias("label_2"),
        "opened_on",
    )
)

warehouse_side = df_warehouses.select("city", "wh_code", "opened_on")

unioned = warehouse_rows.union(warehouse_side)

result = (
    unioned
    .groupBy("label")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("opened_on").alias("latest_opened_on"),
    )
    .orderBy("label")
)
