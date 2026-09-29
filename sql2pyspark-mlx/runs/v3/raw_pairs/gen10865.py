from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-07-01", "2025-09-01") & (F.col("revenue") >= 2400) & F.col("sale_date").between("2024-08-11", "2025-08-01"))
    .withColumn("sale_date_day", F.dayofmonth("sale_date"))
)

store_sales_stats = (
    valid_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.sum("units").alias("total_units"),
        F.count("revenue").alias("revenue_count"),
    )
)

result = (
    store_sales_stats
    .filter(F.col("total_units") < 100)
    .orderBy("sale_month")
)
