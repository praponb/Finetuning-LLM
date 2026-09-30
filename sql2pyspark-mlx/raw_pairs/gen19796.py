from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("s.customer_id"),
        F.quarter(F.col("s.started_at")).alias("started_quarter"),
    )
    .agg(
        F.min(F.col("we.event_time")).alias("min_event_time"),
        F.max(F.col("we.event_time")).alias("latest_event_time"),
    )
    .orderBy("min_event_time", "customer_id", "started_quarter")
    .limit(10)
)
