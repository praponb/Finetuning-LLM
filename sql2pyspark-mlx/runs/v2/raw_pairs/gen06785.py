from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.pct_click") > 27)
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("ss.pct_click"),
        F.col("ss.unique_event_types"),
    )
    .orderBy(F.col("country").desc(), "started_at")
)
