from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 5, 6]) & (F.col("store_id") == 6))
    .select("revenue")
)
