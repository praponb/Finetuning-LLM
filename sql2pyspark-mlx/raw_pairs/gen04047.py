from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter((F.col("units") <= 12) & (F.year("sale_date") >= 2024))

agg = (
    filtered_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3900, "high").when(F.col("revenue") >= 2000, "mid").otherwise("low").alias("revenue_bucket"),
        F.when(F.col("units") >= 38, "high").when(F.col("units") >= 13, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    agg
    .filter(F.col("total_revenue") > 14000)
    .select("revenue_bucket", "units_bucket", "total_revenue")
    .orderBy(F.col("revenue_bucket").desc())
)
