from pyspark.sql import functions as F

warehouse_rows = (
    df_warehouses
    .select(F.col("capacity").alias("key_id"), "id", F.lit("warehouse").alias("src"))
)

warehouse_side = (
    df_warehouses
    .filter((F.col("capacity") <= 11768) & (F.col("city") != "Singapore"))
    .select(
        F.col("capacity").alias("key_id"),
        "id",
        F.lit("warehouse_2").alias("src"),
    )
)

result = warehouse_rows.union(warehouse_side)
