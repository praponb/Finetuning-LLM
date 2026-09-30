from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 1) & (F.col("units") == 33))
    .select("id", "store_id", "sale_date")
    .orderBy("sale_date")
)
