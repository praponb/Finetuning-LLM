from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(F.col("session_id").isin([7, 20]))
    .groupBy("session_id")
    .agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("t"), F.col("s.id") == F.col("t.session_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("t.purchase_count"),
        F.col("t.duration_sec_range"),
    )
    .orderBy(F.col("purchase_count").desc())
)
