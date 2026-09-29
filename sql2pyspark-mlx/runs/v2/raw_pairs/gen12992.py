from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .filter(F.col("s.region") != "Central")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("s.city"),
    )
)
