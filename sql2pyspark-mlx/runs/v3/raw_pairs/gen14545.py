from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id").isin([1, 3, 5, 6]) & F.col("revenue").between(2600, 3200)) | (F.col("sale_date") <= "2025-01-22"))
    .agg(
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.count("units").alias("units_count"),
    )
    .orderBy(F.col("stddev_revenue").desc())
)
