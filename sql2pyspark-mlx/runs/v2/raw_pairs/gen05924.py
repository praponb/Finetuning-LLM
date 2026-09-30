from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("s.region"),
    )
    .orderBy("last_restock", "id")
    .limit(10)
)
