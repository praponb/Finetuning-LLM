from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "inner")
    .filter(F.col("sessions.customer_id").isin([3, 20, 22, 28]) & F.col("web_events.page").isin(["/cart", "/search", "/checkout"]))
    .select(
        F.col("sessions.customer_id"),
        F.col("sessions.browser"),
        F.col("web_events.duration_sec"),
        F.col("web_events.event_time"),
        F.col("web_events.event_type"),
    )
    .orderBy("customer_id")
)
