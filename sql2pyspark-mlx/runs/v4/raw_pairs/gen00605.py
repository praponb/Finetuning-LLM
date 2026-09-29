from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", "product_id", F.year("sale_date").alias("sale_year"))
    .agg(F.min("revenue").alias("min_revenue"))
    .orderBy("product_id")
)
