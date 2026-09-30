from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("duration_sec").between(268, 303))
    .groupBy("session_id")
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .filter(F.col("ss.duration_sec_count") >= 5)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("ss.unique_pages"),
        F.col("ss.duration_sec_count"),
    )
)
