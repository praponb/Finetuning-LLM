from pyspark.sql import functions as F

store_stats = (
    df_inventory
    .filter(F.col("quantity") <= 92)
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("ss.max_quantity"),
    )
    .orderBy(F.col("id").desc())
)
