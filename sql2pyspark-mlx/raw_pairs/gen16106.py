from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("we.event_type"),
        F.col("s.started_at"),
        F.col("c.age"),
        F.concat(F.col("we.event_type"), F.lit("/"), F.col("we.page")).alias("event_type_page"),
    )
    .orderBy("event_type_page", "started_at")
)
