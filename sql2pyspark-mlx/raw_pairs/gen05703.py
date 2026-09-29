from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 4) & F.col("sale_date").between("2024-04-01", "2024-11-01") & F.col("product_id").isin([1, 4, 7, 8]))
    .select("store_id", "units", "revenue")
    .orderBy(F.col("revenue").desc())
)
