from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .select(
        F.lit("warehouse").alias("record_type"),
        F.col("id").alias("qty"),
        "capacity",
        "opened_on",
    )
)

warehouse_side_2 = (
    df_warehouses
    .filter(F.col("capacity") >= 4912)
    .select(
        F.lit("warehouse_2").alias("record_type"),
        "id",
        "capacity",
        "opened_on",
    )
)

stacked = warehouse_side.union(warehouse_side_2)

result = (
    stacked
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("opened_on").alias("latest_opened_on"),
    )
    .orderBy("record_type")
)
