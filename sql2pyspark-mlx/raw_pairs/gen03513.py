from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .filter(F.col("webs.pct_purchase") > 40)
    .select(F.col("s.id"), F.col("s.started_at"), F.col("webs.pct_purchase"))
    .orderBy("started_at")
)
