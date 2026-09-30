from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.hour(F.col("we.event_time")).between(6, 13) & (F.col("s.country") == "US"))
    .select(
        F.col("s.browser"),
        F.col("we.session_id"),
        F.col("we.page"),
        F.col("we.event_time"),
        F.datediff(F.current_date(), F.col("s.started_at")).alias("days_since_started_at"),
    )
    .orderBy(F.col("browser").desc())
)
