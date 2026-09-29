from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("we.duration_sec") < 256) & F.col("c.email").isNotNull())
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
        F.col("s.customer_id"),
        F.col("c.age"),
    )
    .orderBy(F.col("id").desc())
)
