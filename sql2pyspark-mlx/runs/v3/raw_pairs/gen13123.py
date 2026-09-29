from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("quantity") > 158)
    .groupBy("store_id")
    .agg(F.min("last_restock").alias("first_last_restock"))
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.city"),
        F.col("store_stats.first_last_restock"),
    )
    .orderBy(F.col("id").desc())
)
