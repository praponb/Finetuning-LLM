from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
    .orderBy("product_id")
)
