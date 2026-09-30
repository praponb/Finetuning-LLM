from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left_anti")
    .select(
        F.col("sessions.id"),
        F.col("sessions.device"),
        F.col("sessions.customer_id"),
        F.col("sessions.browser"),
    )
    .orderBy(F.col("id").desc())
)
