from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") <= 2200) & (F.col("sale_date") >= "2024-09-01"))
    .select("id", "sale_date", "product_id")
)
