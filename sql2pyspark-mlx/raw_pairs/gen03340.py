from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("t"), F.col("stores.id") == F.col("t.store_id"), "left")
    .select(
        F.col("stores.id"),
        F.col("stores.sqft"),
        F.col("t.latest_last_restock"),
        F.col("t.unique_stores"),
    )
)
