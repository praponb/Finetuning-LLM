from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "add_to_cart"]))
    .groupBy("session_id")
    .agg(F.count("*").alias("num_web_events"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.num_web_events") >= 9)
    .select(F.col("s.browser"), F.col("webs.num_web_events"))
)
