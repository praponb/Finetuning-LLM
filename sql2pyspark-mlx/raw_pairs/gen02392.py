from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_web_events.alias("web_events"), F.col("sessions.id") == F.col("web_events.session_id"), "left")
    .groupBy(F.col("sessions.id"), F.col("sessions.browser"))
    .agg(
        F.count(F.col("web_events.id")).alias("web_event_count"),
        F.count(F.when(F.col("web_events.event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        F.countDistinct(F.col("web_events.duration_sec")).alias("unique_event_types"),
    )
    .filter(F.col("web_event_count") > 2)
    .orderBy("browser")
)
