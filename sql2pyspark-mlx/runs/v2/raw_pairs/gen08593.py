from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .groupBy("store_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.store_name"),
        F.col("store_stats.total_quantity"),
    )
    .orderBy(F.col("id").desc(), F.col("total_quantity").desc())
)
