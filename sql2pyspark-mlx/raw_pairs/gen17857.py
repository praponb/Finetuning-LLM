from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id").isin([1, 4, 5, 6]) & F.col("revenue").isNull()) | (F.col("revenue") >= 1100))
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.max("revenue").alias("max_revenue"),
    )
)
