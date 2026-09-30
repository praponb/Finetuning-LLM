from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("quantity") <= 185)
    .groupBy("store_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("t.total_quantity"),
        F.col("t.last_last_restock"),
    )
    .orderBy("region")
)
