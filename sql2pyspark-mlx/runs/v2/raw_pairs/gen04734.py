from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") >= "2024-01-20") | F.col("store_id").isin([18, 19, 28, 33]))
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"),
        F.avg("discount").alias("avg_discount"),
    )
)
