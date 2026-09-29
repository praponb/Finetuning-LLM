from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("product_id", "units")
    .orderBy(F.col("product_id").desc(), "units")
)
