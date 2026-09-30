from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .select("id", "revenue", "product_id")
    .orderBy(F.col("id").desc(), F.col("product_id").desc())
)
