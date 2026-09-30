from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 2)
    .select("product_id", "revenue", "units")
)
