from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.hour("event_time").between(6, 9))
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_totals.alias("st"), F.col("s.id") == F.col("st.session_id"), "inner")
    .filter(F.col("st.stddev_duration_sec") > 70)
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("st.stddev_duration_sec"),
        F.col("st.duration_sec_count"),
    )
    .orderBy(F.col("stddev_duration_sec").desc())
)
