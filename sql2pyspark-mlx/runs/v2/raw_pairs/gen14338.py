from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("sessions.customer_id") == 26)
    .groupBy(F.quarter(F.col("customers.signup_date")).alias("signup_quarter"))
    .agg(
        F.count(F.when(F.col("web_events.page") == "/cart", 1)).alias("cart_count"),
        F.avg(F.col("web_events.duration_sec")).alias("avg_duration_sec"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
