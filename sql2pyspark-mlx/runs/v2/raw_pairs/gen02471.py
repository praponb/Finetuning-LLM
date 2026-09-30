from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("sessions.browser").isin(["Safari", "Chrome"]))
    .groupBy(F.col("customers.country"))
    .agg(F.max(F.col("web_events.duration_sec")).alias("max_duration_sec"))
)
