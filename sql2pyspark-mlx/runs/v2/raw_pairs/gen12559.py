from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2025-02-01", "2025-02-12"))
    .select("id", "store_id", "revenue", "product_id")
)
