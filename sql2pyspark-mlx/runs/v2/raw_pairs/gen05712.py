from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "right")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("sessions.device"))
    .agg(
        F.countDistinct(F.col("web_events.duration_sec")).alias("unique_sessions"),
        F.sum(F.when(F.col("web_events.page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
    .orderBy("device")
)
