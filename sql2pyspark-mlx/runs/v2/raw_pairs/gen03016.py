from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 5, 6, 7]))
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.sum("revenue").alias("total_revenue"),
    )
)
