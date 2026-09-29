from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("t.first_sale_date"),
        F.col("t.total_count"),
    )
    .orderBy("first_sale_date")
)
