from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.max("revenue").alias("max_revenue"),
    )
    .orderBy("avg_revenue", F.col("store_id").desc())
)
