from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") > "2023-10-25") & F.col("sale_date").between("2023-10-01", "2025-08-01"))
    .select("sale_date", "product_id", "units")
)
