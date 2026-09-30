from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cart"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.duration_sec_range") >= 77)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("webs.duration_sec_range"),
        F.col("webs.pct_cart"),
    )
    .orderBy("duration_sec_range")
)
