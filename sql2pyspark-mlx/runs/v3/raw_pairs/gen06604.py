from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-04-13", "2024-05-18"))
    .groupBy("store_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("store_stats"), F.col("stores.id") == F.col("store_stats.store_id"), "inner")
    .select(
        F.col("stores.opened_date"),
        F.col("stores.store_name"),
        F.col("store_stats.latest_sale_date"),
    )
    .orderBy(F.col("latest_sale_date").desc())
)
