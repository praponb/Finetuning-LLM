from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.count("revenue").alias("revenue_count"),
        F.min("revenue").alias("min_revenue"),
    )
)
