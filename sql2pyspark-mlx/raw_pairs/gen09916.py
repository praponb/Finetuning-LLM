from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 5, 6, 8]) & F.col("product_id").isin([1, 4, 10]))
    .select("sale_date")
    .orderBy(F.col("sale_date").desc())
)
