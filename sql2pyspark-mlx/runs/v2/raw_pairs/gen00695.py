from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(3100, 3200) & F.col("store_id").isin([1, 2]))
    .groupBy("product_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.count("units").alias("units_count"),
    )
    .filter(F.col("units_count") >= 3)
    .orderBy(F.col("avg_revenue").desc())
)
