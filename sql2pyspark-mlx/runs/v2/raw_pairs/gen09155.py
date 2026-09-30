from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter(F.col("product_id") == 3)
    .withColumn("revenue_int", F.col("revenue").cast("int"))
)

sale_month_summary = (
    valid_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"), "store_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = sale_month_summary
