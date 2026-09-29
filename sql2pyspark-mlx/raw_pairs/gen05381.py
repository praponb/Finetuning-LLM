from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "outer")
    .filter(F.col("sessions.started_at") >= "2023-04-14")
    .groupBy(
        F.when(F.col("sessions.country").isin(["JP", "UK"]), "tier_1").otherwise("other").alias("country_group"),
        F.col("sessions.customer_id"),
    )
    .agg(
        (F.max(F.col("web_events.duration_sec")) - F.min(F.col("web_events.duration_sec"))).alias("duration_sec_range"),
    )
    .orderBy(F.col("duration_sec_range").desc())
)
