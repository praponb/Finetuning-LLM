from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)
