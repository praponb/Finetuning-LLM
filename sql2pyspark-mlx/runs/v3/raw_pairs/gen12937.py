from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter((F.col("ss.revenue") >= 2100) & F.col("s.opened_date").between("2024-01-01", "2025-05-01"))
    .select(F.col("s.opened_date"), F.col("ss.product_id"), F.col("ss.units"))
)
