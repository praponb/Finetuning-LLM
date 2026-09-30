from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") != 32) & F.col("revenue").between(1200, 1600))
    .select("id", "product_id", "revenue", "units")
)
