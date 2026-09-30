from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.col("page") == "/search") & ((F.col("page") == "/search") | (F.col("session_id") == 5))))
    .groupBy("event_type")
    .agg(
        F.max("event_time").alias("latest_event_time"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .orderBy(F.col("latest_event_time").desc(), "event_type")
)
