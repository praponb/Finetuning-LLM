from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.countDistinct("revenue").alias("unique_products"))
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("store_totals"), F.col("stores.id") == F.col("store_totals.store_id"), "inner")
    .filter(F.col("store_totals.unique_products") >= 4)
    .select(
        F.col("stores.id"),
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
        F.col("store_totals.unique_products"),
    )
    .orderBy("unique_products", F.col("id").desc())
)
