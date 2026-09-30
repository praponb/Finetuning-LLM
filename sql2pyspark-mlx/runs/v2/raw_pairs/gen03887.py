from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.hour(F.col("s.started_at")).between(8, 11) & F.col("we.page").isNotNull())
    .select(
        F.col("s.country"),
        F.col("we.page"),
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.upper(F.col("we.page")).alias("page_upper"),
    )
    .orderBy("event_type")
)
