from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2024-06-01")
    .groupBy("store_id", "product_id")
    .agg(F.min("sale_date").alias("first_sale_date"))
    .orderBy(F.col("first_sale_date").desc())
)
