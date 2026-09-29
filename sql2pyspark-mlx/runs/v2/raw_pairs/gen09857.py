from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 4]) & (F.col("product_id") == 5))
    .select("product_id", "revenue", "store_id")
    .orderBy(F.col("store_id").desc(), F.col("revenue").desc())
)
