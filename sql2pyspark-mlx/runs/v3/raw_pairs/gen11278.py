from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "right")
    .groupBy(
        F.col("customers.country"),
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
    )
    .agg(F.avg(F.col("web_events.duration_sec")).alias("avg_duration_sec"))
)
