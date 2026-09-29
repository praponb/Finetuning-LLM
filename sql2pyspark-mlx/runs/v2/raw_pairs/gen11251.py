from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2025-05-20")
    .orderBy(F.col("product_id").desc(), F.col("sale_date").desc())
)
