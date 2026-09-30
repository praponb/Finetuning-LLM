from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 4, 5]) & F.col("product_id").isin([5, 6, 8]) & (F.year("sale_date") == 2024))
    .select("id", "store_id", F.last_day("sale_date").alias("sale_date_month_end"))
)
