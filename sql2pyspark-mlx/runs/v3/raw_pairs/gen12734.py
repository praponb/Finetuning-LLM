from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "left")
    .filter(F.col("sessions.country").isin(["US", "DE", "SG"]) & F.col("sessions.customer_id").isin([28, 31]))
    .select(
        F.col("web_events.duration_sec"),
        F.col("sessions.customer_id"),
        F.col("sessions.device"),
        F.when(F.col("web_events.event_type").isin(["click", "purchase"]), "Tier 1").otherwise("Other").alias("event_type_group"),
    )
    .orderBy(F.col("duration_sec").desc())
)
