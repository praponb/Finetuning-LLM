from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .select(
        F.col("store_sales.units"),
        F.col("store_sales.product_id"),
        F.col("store_sales.revenue"),
        F.col("stores.opened_date"),
    )
)
