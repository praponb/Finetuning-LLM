from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter((F.col("revenue") < 3100) | F.col("product_id").isin([1, 4, 8, 10]))
    .withColumn("sale_date_minus_26d", F.date_sub("sale_date", 26))
)

store_sales_stats = (
    filtered_store_sales
    .groupBy("product_id", "store_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.min("sale_date_minus_26d").alias("first_sale_date_minus_26d"),
    )
)

result = store_sales_stats.orderBy("first_sale_date_minus_26d")
