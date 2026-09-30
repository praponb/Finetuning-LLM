from pyspark.sql import functions as F

store_stats = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 6]))
    .groupBy("store_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.max("sale_date").alias("last_sale_date"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("t.max_revenue"),
        F.col("t.last_sale_date"),
    )
    .orderBy("city", "store_name")
)
