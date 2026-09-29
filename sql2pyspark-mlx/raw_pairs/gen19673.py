from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2023-07-01")
    .select(
        "event_type",
        "session_id",
        "event_time",
        F.floor("duration_sec").alias("duration_sec_whole"),
    )
)
