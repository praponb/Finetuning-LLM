from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") < 13) & F.col("sale_date").between("2023-12-05", "2025-06-01") & (F.col("sale_date") <= "2023-09-01"))
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"), "store_id")
    .agg(F.count("revenue").alias("revenue_count"))
)
