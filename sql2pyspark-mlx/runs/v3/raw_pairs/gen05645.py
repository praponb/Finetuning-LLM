from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .groupBy(F.col("sessions.country"))
    .agg(F.max(F.col("web_events.duration_sec")).alias("max_duration_sec"))
    .filter(F.col("max_duration_sec") > 400)
)
