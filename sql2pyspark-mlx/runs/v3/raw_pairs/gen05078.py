from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter((F.to_date(F.col("s.started_at")) >= "2024-07-01") & (F.col("we.event_time") >= "2024-12-20"))
    .select(
        F.col("we.page"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("c.name"),
        F.col("c.segment"),
    )
)
