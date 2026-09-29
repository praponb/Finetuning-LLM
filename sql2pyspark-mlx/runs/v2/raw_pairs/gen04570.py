from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/cart", "/checkout"]))
    .groupBy("session_id")
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("W"), F.col("s.id") == F.col("W.session_id"), "inner")
    .select(F.col("s.id"), F.col("s.country"), F.col("W.unique_event_types"))
    .orderBy("country")
)
