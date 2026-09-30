from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .groupBy("session_id")
    .agg(F.countDistinct("duration_sec").alias("unique_pages"))
)

result = (
    df_sessions.alias("sessions")
    .join(session_summary.alias("SessionSummary"), F.col("sessions.id") == F.col("SessionSummary.session_id"), "left")
    .filter(F.col("SessionSummary.unique_pages") > 4)
    .select(F.col("sessions.customer_id"), F.col("SessionSummary.unique_pages"))
    .orderBy(F.col("customer_id").desc(), F.col("unique_pages").desc())
)
