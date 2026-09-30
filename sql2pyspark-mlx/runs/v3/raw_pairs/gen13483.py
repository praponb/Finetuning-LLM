from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1900, 3500))
    .groupBy("store_id")
    .agg(
        F.count("revenue").alias("revenue_count"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)
