from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 5, 6, 7]))
    .orderBy("sale_date", F.col("store_id").desc())
)
