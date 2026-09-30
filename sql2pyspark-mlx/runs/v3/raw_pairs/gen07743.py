from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-03-01", "2024-08-01"))
    .withColumn("sale_date_label", F.date_format("sale_date", "yyyy-MM-dd"))
)

sale_month_summary = (
    filtered_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.avg("units").alias("avg_units"))
)

result = sale_month_summary.select("sale_month", "avg_units")
