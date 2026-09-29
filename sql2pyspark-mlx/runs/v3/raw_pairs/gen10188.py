from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-10-01", "2024-07-01"))
    .groupBy(
        "product_id",
        F.when(F.col("units") >= 37, "high").when(F.col("units") >= 16, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.avg("revenue").alias("avg_revenue"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("revenue_count") < 3)
    .drop("revenue_count")
)
