from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .select(
        F.col("we.event_time"),
        F.col("we.session_id"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("c.name"),
        F.col("c.country"),
        F.trim(F.col("s.country")).alias("clean_country"),
    )
    .orderBy(F.col("session_id").desc())
)
