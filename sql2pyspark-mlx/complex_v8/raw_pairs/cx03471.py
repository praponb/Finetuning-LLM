from pyspark.sql import functions as F

cleaned_warehouses = (
    df_warehouses
    .filter((F.col("opened_on") < F.lit("2025-02-14").cast("date")) & (F.col("city").isin("Bangkok", "Kuala Lumpur")))
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
    )
)

warehouse_rows = (
    cleaned_warehouses
    .select(
        F.lit("warehouse").alias("record_type"),
        "capacity",
        "wh_code",
        F.col("capacity_filled").alias("key_id"),
    )
)

warehouse_rows_2 = (
    df_warehouses
    .filter(F.col("opened_on") < F.lit("2025-07-03").cast("date"))
    .select(
        F.lit("warehouse_2").alias("record_type"),
        F.col("id").alias("capacity"),
        "wh_code",
        F.col("capacity").alias("key_id"),
    )
)

result = warehouse_rows.unionByName(warehouse_rows_2)
