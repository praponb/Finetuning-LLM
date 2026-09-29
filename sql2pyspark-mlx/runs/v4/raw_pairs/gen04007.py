from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.min("event_time").alias("first_event_time"),
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
    )
    .filter(F.col("product_count") < 52)
    .drop("product_count")
    .orderBy(F.col("first_event_time").desc(), F.col("min_duration_sec").desc())
)
