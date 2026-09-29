from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .select(
        F.col("capacity").alias("ref_id"),
        F.col("opened_on").alias("day"),
        F.col("id").alias("ref_id_2"),
    )
)

warehouse_side_2 = df_warehouses.select("id", "opened_on", "capacity")

result = (
    warehouse_side
    .union(warehouse_side_2)
    .orderBy("day", "ref_id")
)
