from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "search", "page_view"]))
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("web_events_by_session"), F.col("sessions.id") == F.col("web_events_by_session.session_id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("web_events_by_session.total_duration_sec"),
        F.col("web_events_by_session.page_list"),
    )
    .orderBy(F.col("id").desc())
)
