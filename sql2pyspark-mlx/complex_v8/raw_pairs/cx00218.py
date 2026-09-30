from pyspark.sql import functions as F

scoped_warehouses = (
    df_warehouses
    .filter((F.col("city").isin("Bangkok", "Kuala Lumpur")) & (F.year(F.col("opened_on")) == 2025))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.substring(F.trim(F.col("city")), 1, 3).alias("city_prefix"),
    )
)

warehouse_side = (
    scoped_warehouses
    .filter(F.col("city_prefix").isNotNull())
    .select("id", F.col("capacity").alias("key_id"))
)

move_rows = df_stock_moves.select(F.col("warehouse_id").alias("id"), F.col("qty").alias("key_id"))

result = (
    warehouse_side
    .union(move_rows)
    .orderBy(F.col("key_id").desc())
)
