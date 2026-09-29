from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("page").isNull())
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "inner")
    .filter(F.col("t.min_duration_sec") > 220)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("t.page_view_duration_sec"),
        F.col("t.min_duration_sec"),
    )
    .orderBy("browser")
)
