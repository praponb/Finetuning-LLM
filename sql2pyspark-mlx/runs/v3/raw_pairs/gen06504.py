from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.min("revenue").alias("min_revenue"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("t"), F.col("stores.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.min_revenue") > 3200)
    .select(
        F.col("stores.store_name"),
        F.col("stores.opened_date"),
        F.col("t.unique_products"),
        F.col("t.min_revenue"),
    )
    .orderBy(F.col("unique_products").desc(), F.col("store_name").desc())
)
