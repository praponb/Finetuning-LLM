from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("ss.product_id") == 9) & F.col("ss.sale_date").between("2023-12-01", "2025-05-01"))
    .select(
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("s.region"),
        F.col("p.stock_qty"),
    )
)
