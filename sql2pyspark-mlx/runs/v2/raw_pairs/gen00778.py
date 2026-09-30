from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("units").between(15, 32) & F.col("store_id").isin([3, 5]))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 6))
)

summary = (
    filtered_store_sales
    .groupBy("product_id", F.month("sale_date").alias("sale_month"))
    .agg(F.avg("units").alias("avg_units"))
)

result = summary.orderBy(F.col("sale_month").desc(), "product_id")
