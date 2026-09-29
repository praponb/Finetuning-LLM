from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter((F.col("ss.revenue") > 3900) & F.col("ss.sale_date").between("2023-08-01", "2025-01-01"))
    .select(
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("s.store_name"),
    )
    .orderBy("revenue")
)
