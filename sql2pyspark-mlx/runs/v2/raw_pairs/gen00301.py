from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.max("revenue").alias("max_revenue"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("t"), F.col("stores.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.unique_products") > 4)
    .select(
        F.col("stores.id"),
        F.col("stores.city"),
        F.col("t.unique_products"),
        F.col("t.max_revenue"),
    )
    .orderBy(F.col("max_revenue").desc(), "id")
    .limit(5)
)
