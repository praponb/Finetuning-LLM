from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 7]))
    .select("id", "revenue")
    .orderBy(F.col("id").desc())
)
