from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter((F.col("units") <= 17) & F.col("sale_date").between("2023-05-08", "2023-05-16"))
    .withColumn("sale_date_period", F.date_trunc("week", "sale_date"))
)

sale_period_month_summary = (
    src
    .groupBy(F.date_trunc("MONTH", "sale_date_period").alias("sale_period_month"))
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.max("revenue").alias("max_revenue"),
    )
)

result = (
    sale_period_month_summary
    .select("sale_period_month", "max_revenue")
    .orderBy("max_revenue")
)
