from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 3200)
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .filter(F.col("avg_revenue") >= 3200)
)
