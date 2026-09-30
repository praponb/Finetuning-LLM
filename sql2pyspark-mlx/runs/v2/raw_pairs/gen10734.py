from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("duration_sec") > 183)
    .groupBy("session_id")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "inner")
    .filter(F.col("ss.duration_sec_range") >= 79)
    .select(F.col("s.customer_id"), F.col("s.country"), F.col("ss.duration_sec_range"))
    .orderBy("country")
)
