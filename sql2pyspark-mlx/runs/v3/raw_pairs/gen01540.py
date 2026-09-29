from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "inner")
    .select(
        F.col("web_events.session_id"),
        F.col("web_events.event_type"),
        F.col("sessions.country"),
        F.col("sessions.browser"),
        F.col("sessions.customer_id"),
    )
    .orderBy("country")
)
