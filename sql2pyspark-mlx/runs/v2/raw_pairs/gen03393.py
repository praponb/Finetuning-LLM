from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", "product_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
    .orderBy("first_sale_date")
)
