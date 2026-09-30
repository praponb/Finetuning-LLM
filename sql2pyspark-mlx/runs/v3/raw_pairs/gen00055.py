from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.month("sale_date").isin([7, 10, 11]) & (F.col("revenue") >= 1600)) | F.col("sale_date").between("2024-08-01", "2024-11-01"))
    .groupBy(
        "product_id",
        F.when(F.col("revenue") >= 3600, "high").when(F.col("revenue") >= 2000, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.count("units").alias("units_count"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy(F.col("stddev_revenue").desc())
)
