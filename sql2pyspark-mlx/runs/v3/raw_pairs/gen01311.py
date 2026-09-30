from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .groupBy(F.col("sessions.device"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("web_events.event_type")))).alias("event_type_values"),
    )
    .orderBy(F.col("device").desc())
)
