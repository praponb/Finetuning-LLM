from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "right")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("customers.city").isin(["Singapore", "New York", "Chiang Mai"]))
    .groupBy(F.year(F.col("customers.signup_date")).alias("signup_year"))
    .agg(F.countDistinct(F.col("web_events.duration_sec")).alias("unique_sessions"))
    .orderBy("unique_sessions")
)
