from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("opened_date") < "2024-08-01").alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("i.product_id"),
    )
)
