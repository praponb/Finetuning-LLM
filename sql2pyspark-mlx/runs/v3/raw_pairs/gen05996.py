from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.age") != 65)
    .groupBy(F.col("sessions.browser"))
    .agg(
        F.sum(F.when(F.col("web_events.page") == "/search", F.col("web_events.duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
)
