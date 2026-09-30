from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.page") != "/search")
    .select(
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("we.event_type"),
        F.col("we.page"),
        F.col("we.duration_sec"),
        (F.datediff(F.current_date(), F.col("we.event_time")) / 365).alias("years_since_event_time"),
    )
    .orderBy(F.col("years_since_event_time").desc())
)
