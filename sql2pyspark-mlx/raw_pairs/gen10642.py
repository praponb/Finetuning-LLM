from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
)

result = (
    df_stores.alias("stores")
    .join(store_sales_by_store.alias("store_sales_by_store"), F.col("stores.id") == F.col("store_sales_by_store.store_id"), "inner")
    .select(
        F.col("stores.id"),
        F.col("stores.region"),
        F.col("stores.opened_date"),
        F.col("store_sales_by_store.first_sale_date"),
    )
)
