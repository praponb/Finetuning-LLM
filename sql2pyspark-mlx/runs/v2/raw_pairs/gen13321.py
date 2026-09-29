from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("web_events.event_type") != "click")
    .groupBy(
        F.year(F.col("customers.signup_date")).alias("signup_year"),
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
    )
    .agg(
        (F.max(F.col("web_events.duration_sec")) - F.min(F.col("web_events.duration_sec"))).alias("duration_sec_range"),
        F.countDistinct(F.col("web_events.duration_sec")).alias("unique_sessions"),
    )
    .filter(F.col("unique_sessions") > 4)
    .orderBy("signup_month")
)
