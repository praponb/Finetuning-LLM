from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.page").isNotNull())
    .select(
        F.col("we.session_id"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.concat(F.col("we.page"), F.lit(" - "), F.col("we.event_type")).alias("page_event_type"),
    )
    .orderBy(F.col("browser").desc(), "session_id")
)
