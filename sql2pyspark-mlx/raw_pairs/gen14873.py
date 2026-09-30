from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(F.min("event_time").alias("first_event_time"))
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("t.first_event_time"))
)
