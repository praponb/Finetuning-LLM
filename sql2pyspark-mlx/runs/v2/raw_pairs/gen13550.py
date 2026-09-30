from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("ss.duration_sec_count"),
        F.col("ss.purchase_count"),
    )
    .orderBy("browser")
)
