from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .filter(F.col("store_sales.units").between(14, 32) & F.col("store_sales.product_id").isin([1, 3, 5, 9]))
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("store_sales.product_id"),
        F.col("stores.region"),
    )
    .orderBy(F.col("region").desc(), "id")
    .limit(50)
)
