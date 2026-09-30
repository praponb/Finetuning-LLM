from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 1200)
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.sum("revenue").alias("total_revenue"),
        F.max("sale_date").alias("last_sale_date"),
    )
)
