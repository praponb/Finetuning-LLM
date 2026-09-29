from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 10)
    .select("units")
    .orderBy(F.col("units").desc())
)
