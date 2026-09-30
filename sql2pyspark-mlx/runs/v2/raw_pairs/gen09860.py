from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date").between("2024-03-01", "2025-07-01"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.col("s.store_name"),
    )
)
