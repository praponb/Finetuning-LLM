from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("duration_sec") < 155)
    .groupBy("session_id")
    .agg(F.countDistinct("duration_sec").alias("unique_pages"))
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.unique_pages") >= 6)
    .select(
        F.col("sessions.browser"),
        F.col("sessions.customer_id"),
        F.col("t.unique_pages"),
    )
)
