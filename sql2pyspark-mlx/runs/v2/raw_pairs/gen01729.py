from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .groupBy("session_id")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("session_totals"), F.col("sessions.id") == F.col("session_totals.session_id"), "inner")
    .select(F.col("sessions.customer_id"), F.col("session_totals.stddev_duration_sec"))
    .orderBy(F.col("customer_id").desc(), F.col("stddev_duration_sec").desc())
)
