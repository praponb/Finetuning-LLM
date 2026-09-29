from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4]))
    .groupBy("store_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.count("*").alias("store_sale_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("ss.max_revenue"),
        F.col("ss.store_sale_count"),
    )
    .orderBy("store_name", "id")
    .limit(20)
)
