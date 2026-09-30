from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter(F.col("i.last_restock").between("2024-11-15", "2025-10-01") & F.col("s.region").isNotNull())
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("s.region"),
    )
    .orderBy(F.col("id").desc(), "last_restock")
)
