from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("session_totals"), F.col("sessions.id") == F.col("session_totals.session_id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("sessions.device"),
        F.col("session_totals.event_type_values"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(5)
)
