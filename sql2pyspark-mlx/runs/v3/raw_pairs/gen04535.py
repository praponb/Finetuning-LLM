from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 3)
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.countDistinct("revenue").alias("unique_products"),
    )
)
