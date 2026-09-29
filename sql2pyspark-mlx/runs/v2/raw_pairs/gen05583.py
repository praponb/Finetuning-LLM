from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("duration_sec") > 206)
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(web_events_by_session.alias("t"), F.col("sessions.id") == F.col("t.session_id"), "left")
    .select(
        F.col("sessions.country"),
        F.col("sessions.device"),
        F.col("t.home_count"),
        F.col("t.event_type_values"),
    )
    .orderBy(F.col("device").desc())
)
