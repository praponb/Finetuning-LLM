from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("we.event_type").isin(["page_view", "click", "purchase"]))
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("we.event_time"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("c.age"),
        F.concat(F.col("we.page"), F.lit(", "), F.col("we.event_type")).alias("page_event_type"),
    )
)
