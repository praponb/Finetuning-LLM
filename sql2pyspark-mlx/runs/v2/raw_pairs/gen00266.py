from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("t.latest_last_restock"),
        F.col("t.total_quantity"),
    )
    .orderBy("opened_date", F.col("latest_last_restock").desc())
)
