from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .groupBy(F.col("sessions.browser"))
    .agg(
        F.count(F.col("web_events.id")).alias("web_event_count"),
        F.sort_array(F.collect_set(F.col("web_events.event_type"))).alias("event_type_list"),
        F.min(F.col("web_events.event_time")).alias("first_event_time"),
    )
    .orderBy("browser")
)
