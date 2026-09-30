from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("we.duration_sec") == 271)
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("we.page"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("c.segment"),
        F.lower(F.col("we.event_type")).alias("event_type_lower"),
    )
)
