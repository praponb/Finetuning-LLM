from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 5]) | F.col("product_id").isin([1, 2, 4, 5]))
)

summary = (
    filtered_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.min("sale_date").alias("min_sale_date"))
)

result = summary
