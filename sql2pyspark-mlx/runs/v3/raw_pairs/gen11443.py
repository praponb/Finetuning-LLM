from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 9, 10]) & (F.col("revenue") >= 3700))
    .select("id", "revenue", "units")
    .orderBy("revenue")
)
