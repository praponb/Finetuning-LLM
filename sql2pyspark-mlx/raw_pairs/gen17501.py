from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("s.country"),
        F.col("s.device"),
        F.col("c.credit_limit"),
    )
    .orderBy("device", "country")
)
