from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.purchase_duration_sec") > 46)
    .select(F.col("s.browser"), F.col("s.device"), F.col("webs.purchase_duration_sec"))
    .orderBy(F.col("purchase_duration_sec").desc())
)
