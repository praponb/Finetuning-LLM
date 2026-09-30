from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .select(
        F.col("city").alias("category"),
        F.col("id").alias("key_id"),
        F.col("opened_on").alias("day"),
    )
)

warehouse_side_2 = (
    df_warehouses
    .filter((F.col("wh_code").isNotNull()) & (F.col("city") != "Kuala Lumpur"))
    .select(
        F.col("city").alias("category"),
        F.col("capacity").alias("key_id"),
        F.col("opened_on").alias("day"),
    )
)

result = (
    warehouse_side
    .unionByName(warehouse_side_2)
    .orderBy(F.col("category").desc(), "key_id")
)
