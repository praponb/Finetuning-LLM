from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("last_restock") >= "2023-04-17")
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("S"), F.col("s.id") == F.col("S.store_id"), "inner")
    .select(
        F.col("s.city"),
        F.col("s.region"),
        F.col("S.unique_products"),
        F.col("S.total_quantity"),
    )
    .orderBy("region", "unique_products")
)
