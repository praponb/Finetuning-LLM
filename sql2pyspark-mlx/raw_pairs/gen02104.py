from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.hour(F.col("s.started_at")).between(12, 20))
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("s.device"),
        F.col("c.is_active"),
        F.col("c.age"),
    )
    .orderBy("age")
)
