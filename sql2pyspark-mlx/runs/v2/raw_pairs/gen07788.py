from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.country") == "US") & (F.to_date(F.col("we.event_time")) < "2024-04-13"))
    .select(
        F.col("we.page"),
        F.col("we.session_id"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("c.country"),
        F.col("c.email"),
    )
)
