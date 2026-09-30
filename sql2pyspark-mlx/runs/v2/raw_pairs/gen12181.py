from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.col("product_id").isin([2, 3, 9]) & F.col("sale_date").between("2023-03-01", "2024-09-16")))
    .select("product_id", "units", "revenue")
    .orderBy("revenue")
)
