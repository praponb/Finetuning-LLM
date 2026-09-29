from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.year("sale_date") == 2024)
    .groupBy(
        F.when(F.col("revenue") >= 3300, "high").when(F.col("revenue") >= 1100, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.max("sale_date").alias("latest_sale_date"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .orderBy(F.col("revenue_bucket").desc())
)
