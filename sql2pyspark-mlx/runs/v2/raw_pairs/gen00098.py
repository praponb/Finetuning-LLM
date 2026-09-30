from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(16, 20))
    .groupBy("product_id", F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy(
        F.col("stddev_revenue").desc(),
        F.col("avg_revenue").desc(),
        "product_id",
        "sale_month",
    )
    .limit(10)
)
