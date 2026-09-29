from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
    .orderBy(F.col("last_event_time").desc())
)
