from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id").isin([3, 4, 6]) & (F.col("sale_date") < "2024-10-01")) | (F.col("product_id") == 9))
    .select("units")
    .orderBy("units")
)
