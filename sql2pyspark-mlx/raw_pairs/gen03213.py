from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("sessions.id") == F.col("web_events.session_id"), "inner")
    .groupBy(
        F.when(F.year(F.col("sessions.started_at")) < 2024, "before_2024").when(F.year(F.col("sessions.started_at")) == 2024, "2024").otherwise("after_2024").alias("started_cohort"),
    )
    .agg(
        (F.max(F.col("web_events.duration_sec")) - F.min(F.col("web_events.duration_sec"))).alias("duration_sec_range"),
        F.count(F.when(F.col("web_events.event_type") == "purchase", 1)).alias("purchase_count"),
    )
    .orderBy(F.col("purchase_count").desc())
)
