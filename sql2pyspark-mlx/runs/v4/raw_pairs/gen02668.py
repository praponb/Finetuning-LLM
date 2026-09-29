from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("we.id"),
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.col("s.country"),
        F.col("c.age"),
        F.upper(F.col("c.email")).alias("email_upper"),
    )
)
