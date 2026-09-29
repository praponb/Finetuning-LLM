from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("revenue") > 1800)

store_sales_stats = (
    filtered_store_sales
    .groupBy(
        F.year("sale_date").alias("sale_year"),
        F.when(F.col("revenue") >= 3300, "high").when(F.col("revenue") >= 1900, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.sum("units").alias("total_units"))
)

result = (
    store_sales_stats
    .filter(F.col("total_units") > 120)
    .select("sale_year", "total_units")
)
