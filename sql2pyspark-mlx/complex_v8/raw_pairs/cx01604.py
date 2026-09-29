from pyspark.sql import functions as F

warehouse_side = (
    df_warehouses
    .select("id", F.col("city").alias("name"), F.col("capacity").alias("key_id"))
)

move_side = (
    df_stock_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        F.col("sku_id").alias("id"),
        F.col("move_type").alias("name"),
        F.col("id").alias("key_id"),
    )
)

result = (
    warehouse_side
    .unionByName(move_side)
    .distinct()
    .orderBy(F.col("key_id").desc(), "id")
)
