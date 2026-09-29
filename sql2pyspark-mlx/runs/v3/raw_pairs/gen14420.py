from pyspark.sql import functions as F

valid_store_sales = (
    df_store_sales
    .filter(F.col("revenue").between(1200, 3000) & F.col("sale_date").between("2024-02-04", "2024-10-15"))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 3))
)

summary = (
    valid_store_sales
    .groupBy("store_id")
    .agg(F.count("revenue").alias("revenue_count"))
)

result = summary.filter(F.col("revenue_count") > 6)
