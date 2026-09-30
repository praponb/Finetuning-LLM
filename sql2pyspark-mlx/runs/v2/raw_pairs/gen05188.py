from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .select(
        F.col("we.duration_sec"),
        F.col("we.event_type"),
        F.col("we.page"),
        F.col("s.device"),
        F.col("s.country"),
        F.date_format(F.col("we.event_time"), "yyyy-MM").alias("event_time_label"),
    )
    .orderBy("event_type", F.col("country").desc())
)
