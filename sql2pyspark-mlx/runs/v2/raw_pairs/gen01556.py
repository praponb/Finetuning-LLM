from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
        F.when(F.col("revenue") >= 4100, "high").when(F.col("revenue") >= 1300, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy("latest_sale_date")
)
