from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.filter(F.col("city") == "Tokyo").alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "left_semi")
    .filter(F.col("store_sales.revenue") < 1500)
    .select(
        F.col("store_sales.id"),
        F.col("store_sales.revenue"),
        F.col("store_sales.product_id"),
        F.col("store_sales.sale_date"),
    )
)
