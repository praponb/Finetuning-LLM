from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", "event_type")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
    )
    .filter(F.col("avg_duration_sec") > 340)
    .orderBy("avg_duration_sec")
)
