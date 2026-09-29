from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.year("sale_date") >= 2024)
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.avg("revenue").alias("avg_revenue"),
    )
)
