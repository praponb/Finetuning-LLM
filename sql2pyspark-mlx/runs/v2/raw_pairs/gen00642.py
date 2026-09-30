from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.segment"), F.col("customers.country"))
    .agg(
        F.round(F.avg(F.col("web_events.duration_sec")), 2).alias("avg_duration_sec"),
        F.round(F.stddev(F.col("web_events.duration_sec")), 2).alias("stddev_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") >= 36)
)
