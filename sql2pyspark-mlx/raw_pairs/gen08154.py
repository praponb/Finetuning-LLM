from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 5, 6]))
    .select("id", "product_id")
    .orderBy(F.col("id").desc())
    .limit(10)
)
