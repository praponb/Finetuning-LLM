from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(~F.col("s.browser").isin(["Edge", "Firefox"]))
    .select(
        F.col("s.started_at"),
        F.col("we.page"),
        F.col("we.event_time"),
        F.col("we.event_type"),
    )
    .orderBy(F.col("event_type").desc())
)
