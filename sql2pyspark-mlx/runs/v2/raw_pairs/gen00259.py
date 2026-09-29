from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-04-01", "2024-08-01") & F.col("product_id").isin([1, 4]))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
)

agg = (
    filtered_store_sales
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.count("units").alias("units_count"),
    )
)

result = (
    agg
    .orderBy(F.col("units_count").desc(), F.col("product_id").desc())
    .limit(5)
)
