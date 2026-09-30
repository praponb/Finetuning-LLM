from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 5, 6, 10]) & (F.col("sale_date").between("2023-04-01", "2024-08-01") | F.col("revenue").between(2100, 4000)))
)
