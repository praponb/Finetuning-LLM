from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 5, 7]))
    .orderBy(F.col("product_id").desc())
)
