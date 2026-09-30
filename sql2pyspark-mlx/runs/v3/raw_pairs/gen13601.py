from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.col("product_id").isin([2, 4, 6]) & F.col("product_id").isin([2, 4, 7, 8])))
)
