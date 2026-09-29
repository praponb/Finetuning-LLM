from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("sessions.browser"), F.col("sessions.customer_id"))
    .agg(F.max(F.col("web_events.duration_sec")).alias("max_duration_sec"))
    .orderBy(F.col("max_duration_sec").desc())
)
