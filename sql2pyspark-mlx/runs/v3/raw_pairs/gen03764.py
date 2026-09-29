from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "revenue", "product_id")
    .orderBy(F.col("product_id").desc(), "id")
    .limit(5)
)
