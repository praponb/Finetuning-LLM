from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.year("sale_date").alias("sale_year"),
        F.when(F.col("units") >= 39, "high").when(F.col("units") >= 7, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.count("revenue").alias("revenue_count"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 3)
    .orderBy("total_count")
)
