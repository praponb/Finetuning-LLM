from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("c.age"),
        F.col("c.city"),
    )
    .orderBy("started_at", F.col("city").desc())
)
