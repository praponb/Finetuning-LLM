from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter((F.col("s.sqft") <= 12145) & F.col("i.store_id").isin([1, 2, 4, 5]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.store_name"),
        F.col("s.city"),
    )
    .orderBy("city")
)
