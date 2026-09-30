from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "product_id")
    .agg(F.max("revenue").alias("max_revenue"))
    .filter(F.col("max_revenue") < 3100)
)
