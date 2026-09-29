from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.region") != "Central")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("s.store_name"),
        F.col("s.city"),
    )
    .orderBy(F.col("last_restock").desc(), "id")
    .limit(3)
)
