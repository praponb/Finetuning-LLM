from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)
