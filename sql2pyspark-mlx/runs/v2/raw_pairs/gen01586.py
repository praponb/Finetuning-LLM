from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("opened_date") >= "2025-05-17").alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_anti")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
    )
)
