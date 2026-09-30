from pyspark.sql import functions as F

result = (
    df_web_events.alias("web_events")
    .join(df_sessions.alias("sessions"), F.col("web_events.session_id") == F.col("sessions.id"), "outer")
    .filter(F.col("web_events.duration_sec").between(125, 338))
    .groupBy(F.col("sessions.country"))
    .agg(F.countDistinct(F.col("web_events.duration_sec")).alias("unique_sessions"))
    .orderBy(F.col("unique_sessions").desc())
)
