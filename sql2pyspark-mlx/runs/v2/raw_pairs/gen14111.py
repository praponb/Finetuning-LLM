from pyspark.sql import functions as F

session_totals = (
    df_web_events
    .filter(F.col("page") == "/product")
    .groupBy("session_id")
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
)

result = (
    df_sessions.alias("sessions")
    .join(session_totals.alias("session_totals"), F.col("sessions.id") == F.col("session_totals.session_id"), "inner")
    .select(
        F.col("sessions.customer_id"),
        F.col("session_totals.cnt"),
        F.col("session_totals.event_type_list"),
    )
    .orderBy(F.col("customer_id").desc())
)
