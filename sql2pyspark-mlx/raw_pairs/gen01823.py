from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.device") == "mobile")
    .select(
        F.col("we.event_type"),
        F.col("we.page"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("c.email"),
        F.col("c.age"),
    )
    .orderBy("device")
)
