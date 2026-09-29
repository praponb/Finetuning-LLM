from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("s.device"),
        F.col("s.started_at"),
        F.col("c.city"),
        F.col("c.signup_date"),
        F.coalesce(F.col("c.segment"), F.lit("unassigned")).alias("segment_filled"),
    )
)
