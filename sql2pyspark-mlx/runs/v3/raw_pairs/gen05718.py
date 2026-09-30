from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1600)
    .select("id", "revenue")
)
