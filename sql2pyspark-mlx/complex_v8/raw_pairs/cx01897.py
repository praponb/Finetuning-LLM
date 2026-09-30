from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .filter((F.col("city").isin("Kuala Lumpur", "Singapore")) & (F.col("wh_code").isNotNull()))
    .select(
        F.lit("warehouse").alias("source"),
        F.col("city").alias("name"),
        F.col("id").alias("qty"),
        "opened_on",
    )
)

warehouse_side = (
    df_warehouses
    .select(
        F.lit("warehouse_2").alias("source"),
        F.col("city").alias("name"),
        F.col("id").alias("qty"),
        "opened_on",
    )
)

warehouse_side_2 = (
    df_warehouses
    .filter((F.col("capacity") < 11768) & (F.col("opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-07-03").cast("date"))))
    .select(
        F.lit("warehouse_3").alias("source"),
        F.col("city").alias("name"),
        F.col("capacity").alias("qty"),
        "opened_on",
    )
)

result = (
    warehouse_rows
    .unionByName(warehouse_side)
    .unionByName(warehouse_side_2)
)
