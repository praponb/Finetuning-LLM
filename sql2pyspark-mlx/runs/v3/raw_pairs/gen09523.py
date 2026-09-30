from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/search") & (F.col("duration_sec").between(311, 423) | (F.col("event_time") >= "2025-01-01")))
    .select(
        "id",
        "event_type",
        "duration_sec",
        "event_time",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.hour("event_time").alias("event_time_hour"),
    )
)
