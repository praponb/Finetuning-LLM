from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-05-01", "2025-07-27") & F.col("store_id").isin([1, 3, 4, 5]))
    .select("product_id")
)
