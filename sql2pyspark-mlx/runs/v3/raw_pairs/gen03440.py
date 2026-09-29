from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter((F.col("sessions.device") == "desktop") & F.col("sessions.customer_id").isin([24, 34]))
    .select(
        F.col("web_events.id"),
        F.col("web_events.event_type"),
        F.col("sessions.browser"),
        F.concat(F.col("sessions.device"), F.lit("/"), F.col("sessions.browser")).alias("device_browser"),
    )
    .orderBy("device_browser", "browser", "id")
    .limit(5)
)
