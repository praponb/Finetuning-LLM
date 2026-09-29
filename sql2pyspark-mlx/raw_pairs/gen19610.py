from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.browser") == "Safari")
    .select(
        F.col("s.customer_id"),
        F.col("we.event_time"),
        F.col("we.page"),
        F.col("we.duration_sec"),
    )
    .orderBy(F.col("duration_sec").desc())
)
