from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.col("units") > 28)
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.max("revenue").alias("max_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("ssbs.total_count"),
        F.col("ssbs.max_revenue"),
    )
    .orderBy("total_count")
)
