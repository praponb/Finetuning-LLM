from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("opened_date") < "2025-09-01").alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
    )
)
