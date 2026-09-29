from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.store_id").isin([4, 5]) & F.col("i.store_id").isin([1, 4, 5, 6]))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("s.region"),
    )
    .orderBy("city")
)
