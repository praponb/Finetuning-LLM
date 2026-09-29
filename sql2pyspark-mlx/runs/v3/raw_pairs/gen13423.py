from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 5, 8]) & F.month("sale_date").isin([2, 10, 12]))
)
