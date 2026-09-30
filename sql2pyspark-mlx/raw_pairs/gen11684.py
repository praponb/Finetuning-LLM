from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 6]))
    .groupBy("product_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
)
