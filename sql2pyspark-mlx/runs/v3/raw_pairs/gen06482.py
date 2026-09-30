from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(((F.col("revenue") <= 1700) & (F.col("revenue") >= 1300)) | (F.col("revenue") < 3500))
)

summary = (
    filtered_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3200, "high").when(F.col("revenue") >= 900, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.count("*").alias("num_store_sales"),
    )
)

result = (
    summary
    .select("revenue_bucket", "max_revenue")
    .orderBy(F.col("max_revenue").desc())
)
