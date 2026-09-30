from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "inner")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .groupBy(
        F.year(F.col("sessions.started_at")).alias("started_year"),
        F.date_trunc("MONTH", F.col("sessions.started_at")).alias("started_month"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("web_events.event_type"))).alias("event_type_list"),
    )
    .orderBy("started_year")
)
