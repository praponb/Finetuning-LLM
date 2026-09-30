from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "page_view"]))
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.min("event_time").alias("first_event_time"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("webs.page_values"),
        F.col("webs.first_event_time"),
    )
    .orderBy("browser", "id")
    .limit(10)
)
