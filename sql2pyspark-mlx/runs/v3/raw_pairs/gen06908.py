from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("we.page").isNotNull() | F.col("s.country").isin(["UK", "US"]))
    .select(
        F.col("we.event_type"),
        F.col("we.duration_sec"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
        F.col("c.email"),
        F.col("c.age"),
    )
    .orderBy("event_type")
)
