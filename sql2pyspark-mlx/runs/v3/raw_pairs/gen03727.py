from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .select(F.col("s.browser"), F.col("webs.click_count"))
    .orderBy(F.col("click_count").desc())
)
