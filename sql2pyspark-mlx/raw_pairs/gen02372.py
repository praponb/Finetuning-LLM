from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 8]) & F.col("product_id").isin([1, 2, 5, 9]))
    .select("id", "sale_date")
    .orderBy("id")
    .limit(1)
)
