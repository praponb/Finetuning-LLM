from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.hour(F.col("we.event_time")).between(7, 13) & F.col("s.browser").isNotNull())
    .select(
        F.col("we.page"),
        F.col("we.session_id"),
        F.col("we.duration_sec"),
        F.col("s.browser"),
        F.date_format(F.col("s.started_at"), "yyyy-MM").alias("started_at_label"),
    )
)
