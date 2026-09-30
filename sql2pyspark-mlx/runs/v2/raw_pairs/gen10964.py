from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") == 39) & F.col("units").between(24, 26))
    .select("store_id", "product_id")
)
