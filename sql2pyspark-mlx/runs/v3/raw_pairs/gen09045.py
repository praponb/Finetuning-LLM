from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
)
