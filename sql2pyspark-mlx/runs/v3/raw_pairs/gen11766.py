from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.min("sale_date").alias("min_sale_date"))
)
