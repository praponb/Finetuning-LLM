from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([32, 36]))
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)
