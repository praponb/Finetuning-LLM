from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 9]) & (F.col("product_id") == 4) & (F.col("revenue") <= 1100))
    .select("units")
    .orderBy(F.col("units").desc())
)
