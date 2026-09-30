from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 4, 6]))
    .groupBy("product_id", F.month("sale_date").alias("sale_month"))
    .agg(F.max("sale_date").alias("last_sale_date"))
)
